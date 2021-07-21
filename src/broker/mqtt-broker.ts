import type Logger from 'bunyan'
import {connect, IClientPublishOptions, MqttClient} from 'mqtt'
import {Broker, DeliveryState, SendContext, SendOptions, Updater} from './broker'
import {CancelError, DeliveryError} from './errors'
import {Hash, Hasher} from '../hasher'

interface MqttBrokerOptions {
    /** MQTT server url. */
    mqtt_url: string
    /** How many seconds to ask server to persist messages for. */
    mqtt_expiry?: number
}

interface Waiter {
    channel: string
    hash: Hash
    cb: (error?: Error, hash?: Hash) => void
}

/** Passes messages and delivery notifications over MQTT. */
export class MqttBroker implements Broker {
    private client: MqttClient
    private subscribers: Array<{channel: string; updater: Updater}> = []
    private waiting: Array<Waiter> = []
    private expiry: number
    private ended: boolean
    private hasher = new Hasher()

    constructor(private options: MqttBrokerOptions, private logger: Logger) {
        this.ended = false
        this.expiry = options.mqtt_expiry || 60 * 30
        this.client = connect(options.mqtt_url)
        this.client.on('message', this.messageHandler.bind(this))
        this.client.on('close', () => {
            if (!this.ended) {
                this.logger.warn('disconnected from server')
            }
        })
        this.client.on('connect', () => {
            this.logger.info('connected')
        })
        this.client.on('reconnect', () => {
            this.logger.info('attempt reconnect')
        })
    }

    async init() {
        this.logger.info('mqtt broker init %s', this.options.mqtt_url)
        if (!this.client.connected) {
            await new Promise<void>((resolve, reject) => {
                this.client.once('connect', () => {
                    this.client.removeListener('error', reject)
                    resolve()
                })
                this.client.once('error', reject)
            })
        }
    }

    async deinit() {
        this.logger.debug('mqtt broker deinit')
        this.ended = true
        const cancels: Promise<void>[] = []
        for (const {hash, channel, cb} of this.waiting) {
            this.logger.info({hash, channel}, 'cancelling waiter')
            cancels.push(this.sendCancel(channel, hash))
            cb(new CancelError('Shutting down'))
        }
        this.waiting = []
        await Promise.all(cancels)
        await new Promise<void>((resolve, reject) => {
            this.client.end(false, {}, (error) => {
                if (error) {
                    reject(error)
                } else {
                    resolve()
                }
            })
        })
    }

    async healthCheck() {
        if (!this.client.connected) {
            throw new Error('Lost connection to MQTT server')
        }
    }

    async send(channel: string, payload: Buffer, options: SendOptions, ctx?: SendContext) {
        const hash = this.hasher.hash(payload)
        this.logger.debug({hash, channel}, 'send %s', hash)
        const cancel = () => {
            this.sendCancel(channel, hash).catch((error) => {
                this.logger.warn(error, 'error during send cancel')
            })
        }
        if (ctx) {
            ctx.cancel = cancel
        }
        const timeout = (options.wait || 0) * 1000
        let deliveryPromise: Promise<void> | undefined
        if (timeout > 0) {
            deliveryPromise = this.waitForDelivery(channel, timeout, hash)
            // this shuts node up about "rejection was handled asynchronously"
            // we will handle the error, just not right away
            deliveryPromise.catch(() => {}) // eslint-disable-line @typescript-eslint/no-empty-function
        }
        await this.publish(`channel/${channel}`, payload, {
            qos: 2,
            retain: true,
            properties: {
                messageExpiryInterval: this.expiry,
            },
        })
        let rv = DeliveryState.buffered
        if (deliveryPromise) {
            try {
                await deliveryPromise
                rv = DeliveryState.delivered
            } catch (error) {
                if (ctx) {
                    ctx.cancel = undefined
                }
                if (error instanceof DeliveryError) {
                    if (options.requireDelivery) {
                        cancel()
                        throw error
                    }
                } else {
                    throw error
                }
            }
        }
        if (ctx) {
            ctx.cancel = undefined
        }
        return rv
    }

    async subscribe(channel: string, updater: Updater) {
        this.logger.debug({channel}, 'new subscription')
        const sub = {channel, updater}
        await this.addSubscriber(sub)
        return () => {
            this.logger.debug({channel}, 'unsubscribe')
            this.removeSubscriber(sub)
        }
    }

    private async sendCancel(channel: string, hash?: Hash) {
        // clear channel
        const clear = new Promise<void>((resolve, reject) => {
            this.client.publish(`channel/${channel}`, '', {qos: 1, retain: true}, (error) => {
                if (error) {
                    reject(error)
                } else {
                    resolve()
                }
            })
        })
        // tell other pending sends that we cancelled
        const tell = new Promise<void>((resolve, reject) => {
            this.client.publish(`cancel/${channel}`, hash?.bytes || '', {qos: 1}, (error) => {
                if (error) {
                    reject(error)
                } else {
                    resolve()
                }
            })
        })
        await Promise.all([clear, tell])
    }

    private messageHandler(topic: string, payload: Buffer) {
        const parts = topic.split('/')
        switch (parts[0]) {
            case 'delivery':
                this.handleDelivery(
                    parts[1],
                    payload.byteLength > 0 ? new Hash(payload) : undefined
                )
                break
            case 'channel':
                this.handleChannelMessage(parts[1], payload)
                break
            case 'cancel':
                this.handleCancel(parts[1], payload.byteLength > 0 ? new Hash(payload) : undefined)
                break
            case 'wait':
                this.handleWait(parts[1], new Hash(payload))
                break
            default:
                this.logger.warn({topic}, 'unexpected message')
        }
    }

    private handleChannelMessage(channel: string, payload: Buffer) {
        if (payload.length === 0) {
            return
        }
        const hash = this.hasher.hash(payload)
        const updaters = this.subscribers
            .filter((sub) => sub.channel === channel)
            .map((sub) => sub.updater)
        this.logger.debug({channel, hash}, 'updating %d subscription(s)', updaters.length)
        Promise.allSettled(updaters.map((fn) => fn(payload)))
            .then((results) => results.some((result) => result.status === 'fulfilled'))
            .then((delivered) => {
                if (delivered) {
                    // clear retained message so it's not re-delivered
                    this.client.publish(`channel/${channel}`, '', {qos: 1, retain: true})
                    // tell waiting sends that we delivered
                    this.client.publish(`delivery/${channel}`, hash.bytes, {qos: 1})
                }
            })
            .catch((error) => {
                this.logger.error(error, 'unexpected error during subscriber delivery')
            })
    }

    private handleDelivery(channel: string, hash?: Hash) {
        this.logger.debug({channel, hash}, 'delivery')
        const waiters = this.waiting.filter((item) => item.channel === channel)
        for (const waiter of waiters) {
            waiter.cb(undefined, hash)
        }
    }

    private handleCancel(channel: string, hash?: Hash) {
        this.logger.debug({channel, hash}, 'cancel')
        const waiters = this.waiting.filter((item) => item.channel === channel)
        for (const waiter of waiters) {
            if (!hash || waiter.hash.equals(hash)) {
                waiter.cb(new CancelError('Cancel from other sender'))
            }
        }
    }

    private handleWait(channel: string, hash: Hash) {
        this.logger.debug({channel, hash}, 'wait')
        const waiters = this.waiting.filter((item) => item.channel === channel)
        for (const waiter of waiters) {
            if (!waiter.hash.equals(hash)) {
                waiter.cb(new CancelError('Replaced by newer message'))
            }
        }
    }

    private waitForDelivery(channel: string, timeout: number, hash: Hash) {
        return new Promise<void>((resolve, reject) => {
            this.logger.debug({channel, hash}, 'wait for delivery')
            const topics = [`delivery/${channel}`, `cancel/${channel}`, `wait/${channel}`]
            const cleanup = () => {
                clearTimeout(timer)
                this.waiting.splice(this.waiting.indexOf(waiter), 1)
                const lastWaiter = this.waiting.findIndex((item) => item.channel === channel) === -1
                this.logger.debug({channel, hash, lastWaiter}, 'delivery wait cleanup')
                if (lastWaiter) {
                    this.client.unsubscribe(topics, (error: any) => {
                        if (error) {
                            this.logger.warn(error, 'error during unsubscribe')
                        }
                    })
                }
            }
            this.client.subscribe(topics, {qos: 1}, (error) => {
                if (error) {
                    cleanup()
                    reject(error)
                }
            })
            this.client.publish(`wait/${channel}`, hash.bytes, {qos: 1})
            const timer = setTimeout(() => {
                cleanup()
                reject(new DeliveryError(`Timed out after ${timeout}ms`))
            }, timeout)
            const waiter: Waiter = {
                channel,
                hash,
                cb: (error?: Error, result?: Hash) => {
                    cleanup()
                    if (error) {
                        reject(error)
                    } else {
                        if (result && !result.equals(hash)) {
                            reject(new CancelError('Replaced by other delivery'))
                        } else {
                            resolve()
                        }
                    }
                },
            }
            this.waiting.push(waiter)
        })
    }

    private addSubscriber(sub: {channel: string; updater: Updater}) {
        return new Promise<void>((resolve, reject) => {
            const topic = `channel/${sub.channel}`
            this.client.subscribe(topic, {qos: 2}, (error) => {
                if (error) {
                    reject(error)
                } else {
                    this.subscribers.push(sub)
                    resolve()
                }
            })
        })
    }

    private removeSubscriber(sub: {channel: string; updater: Updater}) {
        const idx = this.subscribers.indexOf(sub)
        if (idx !== -1) {
            this.subscribers.splice(idx, 1)
        }
        const active = this.subscribers.find(({channel}) => sub.channel === channel)
        if (!active) {
            const topic = `channel/${sub.channel}`
            this.logger.debug({topic}, 'unsubscribing from inactive topic')
            this.client.unsubscribe(topic, (error: any) => {
                if (error) {
                    this.logger.warn(error, 'error during channel unsubscribe')
                }
            })
        }
    }

    private async publish(
        topic: string,
        payload: string | Buffer,
        options: IClientPublishOptions = {}
    ) {
        return new Promise<void>((resolve, reject) => {
            this.client.publish(topic, payload, options, (error) => {
                if (error) {
                    reject(error)
                } else {
                    resolve()
                }
            })
        })
    }
}
