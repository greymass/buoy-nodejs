import type Logger from 'bunyan'
import {Broker, DeliveryState, SendContext, SendOptions, Updater} from './broker'
import {CancelError, TimeoutError} from './errors'
import LRUCache from 'lru-cache'

export interface MemoryBrokerOptions {
    /** Max age in milliseconds. */
    memory_max_age?: number
    /** Max size in bytes. */
    memory_max_size?: number
}

export class MemoryBroker implements Broker {
    private cache: LRUCache<string, Buffer>
    private subscribers: Array<{channel: string; updater: Updater}> = []
    private waiting: Map<string, (updater: Updater) => void> = new Map()

    constructor(options: MemoryBrokerOptions, private logger: Logger) {
        this.cache = new LRUCache({
            stale: true,
            maxAge: (options.memory_max_age || 60 * 60) * 1000,
            max: (options.memory_max_size || 500) * 1e6,
            length: (item) => item.byteLength,
        })
    }

    // eslint-disable-next-line @typescript-eslint/no-empty-function
    async init() {}

    // eslint-disable-next-line @typescript-eslint/no-empty-function
    async healthCheck() {}

    async subscribe(channel: string, updater: Updater) {
        this.logger.debug({channel}, 'new subscription')
        const sub = {channel, updater}
        this.subscribers.push(sub)
        setImmediate(() => {
            if (this.cache.has(channel)) {
                this.logger.debug({channel}, 'delivering cached payload')
                sub.updater(this.cache.get(channel)!)
                this.cache.del(channel)
            }
            if (this.waiting.has(channel)) {
                this.logger.debug({channel}, 'resolving waiting delivery')
                this.waiting.get(channel)!(sub.updater)
            }
        })
        return () => {
            const idx = this.subscribers.indexOf(sub)
            this.logger.debug({channel, idx}, 'unsubscribe')
            this.subscribers.splice(idx, 1)
        }
    }

    async send(channel: string, payload: Buffer, options: SendOptions, ctx?: SendContext) {
        let cancelled = false
        if (ctx) {
            ctx.cancel = () => {
                cancelled = true
            }
        }
        try {
            const updaters = await this.waitForSubscribers(channel, (options.wait || 0) * 1000)
            if (cancelled) {
                throw new CancelError()
            }
            this.logger.debug({channel}, 'direct delivery to %d subscriber(s)', updaters.length)
            for (const updater of updaters) {
                updater(payload)
            }
            return DeliveryState.delivered
        } catch (error) {
            if (error instanceof TimeoutError && !options.requireDelivery) {
                if (cancelled) {
                    throw new CancelError()
                }
                this.logger.debug({channel}, 'buffered delivery')
                this.cache.set(channel, payload)
                return DeliveryState.buffered
            } else {
                throw error
            }
        }
    }

    private waitForSubscribers(channel: string, timeout: number) {
        return new Promise<Updater[]>((resolve, reject) => {
            const sub = this.subscribers
                .filter((sub) => sub.channel === channel)
                .map((sub) => sub.updater)
            if (sub.length > 0) {
                resolve(sub)
            } else {
                setTimeout(() => {
                    this.waiting.delete(channel)
                    reject(new TimeoutError(timeout))
                }, timeout)
                this.waiting.set(channel, (updater) => {
                    this.waiting.delete(channel)
                    resolve([updater])
                })
            }
        })
    }
}
