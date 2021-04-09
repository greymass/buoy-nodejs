import type Logger from 'bunyan'
import {connect, IClientPublishOptions, MqttClient} from 'mqtt'
import {Broker, DeliveryState, SendContext, SendOptions, Updater} from './broker'
import {CancelError, TimeoutError} from './errors'

interface MqttBrokerOptions {
    /** MQTT server url. */
    mqtt_url: string
    /** How many seconds to ask server to persist messages for. */
    mqtt_expiry?: number
}

/** Passes messages and delivery notifications over MQTT. */
export class MqttBroker implements Broker {
    private client: MqttClient
    private subscribers: Array<{channel: string; updater: Updater}> = []
    private waiting: Map<string, (error?: Error) => void> = new Map()
    private expiry: number
    private ended: boolean

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
        for (const [channel, fn] of this.waiting) {
            this.logger.info('cancelling %s', channel)
            this.sendCancel(channel)
            fn(new CancelError())
        }
        await new Promise<void>((resolve) => {
            this.client.end(false, {}, resolve)
        })
    }

    async healthCheck() {
        if (!this.client.connected) {
            throw new Error('Lost connection to MQTT server')
        }
    }

    async send(channel: string, payload: Buffer, options: SendOptions, ctx?: SendContext) {
        const cancel = () => {
            this.sendCancel(channel)
        }
        if (ctx) {
            ctx.cancel = cancel
        }
        const timeout = (options.wait || 0) * 1000
        let deliveryPromise: Promise<void> | undefined
        if (timeout > 0) {
            deliveryPromise = this.waitForDelivery(channel, timeout)
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
                if (error instanceof TimeoutError) {
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

    private sendCancel(channel: string) {
        // clear channel
        this.client.publish(`channel/${channel}`, '', {qos: 1, retain: true})
        // tell other pending sends that we cancelled
        this.client.publish(`cancel/${channel}`, '', {qos: 1})
    }

    private messageHandler(topic: string, payload: Buffer) {
        const parts = topic.split('/')
        switch (parts[0]) {
            case 'delivery':
                this.handleDelivery(parts[1])
                break
            case 'channel':
                this.handleChannelMessage(parts[1], payload)
                break
            case 'cancel':
                this.handleCancel(parts[1])
                break
            default:
                this.logger.warn({topic}, 'unexpected message')
        }
    }

    private handleChannelMessage(channel: string, payload: Buffer) {
        if (payload.length === 0) {
            return
        }
        const updaters = this.subscribers
            .filter((sub) => sub.channel === channel)
            .map((sub) => sub.updater)
        this.logger.debug({channel}, 'updating %d subscription(s)', updaters.length)
        for (const updater of updaters) {
            updater(payload as Buffer)
        }
        if (updaters.length > 0) {
            // clear retained message so it's not re-delivered
            this.client.publish(`channel/${channel}`, '', {qos: 1, retain: true})
            // tell waiting sends that we delivered
            this.client.publish(`delivery/${channel}`, '', {qos: 1})
        }
    }

    private handleDelivery(channel: string) {
        const fn = this.waiting.get(channel)
        if (fn) {
            fn()
        }
    }

    private handleCancel(channel: string) {
        const fn = this.waiting.get(channel)
        if (fn) {
            fn(new CancelError())
        }
    }

    private waitForDelivery(channel: string, timeout: number) {
        return new Promise<void>((resolve, reject) => {
            const cleanup = () => {
                this.waiting.delete(channel)
                this.client.unsubscribe(`delivery/${channel}`, (error: any) => {
                    if (error) {
                        this.logger.warn(error, 'error during delivery unsubscribe')
                    }
                })
                this.client.unsubscribe(`cancel/${channel}`, (error: any) => {
                    if (error) {
                        this.logger.warn(error, 'error during delivery unsubscribe')
                    }
                })
            }
            this.client.subscribe(`delivery/${channel}`, {qos: 1}, (error) => {
                if (error) {
                    cleanup()
                    reject(error)
                }
            })
            this.client.subscribe(`cancel/${channel}`, {qos: 1}, (error) => {
                if (error) {
                    cleanup()
                    reject(error)
                }
            })
            setTimeout(() => {
                cleanup()
                reject(new TimeoutError(timeout))
            }, timeout)
            this.waiting.set(channel, (error?: Error) => {
                cleanup()
                if (error) {
                    reject(error)
                } else {
                    resolve()
                }
            })
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
