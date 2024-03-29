export enum DeliveryState {
    /** Delivery confirmed. */
    delivered = 'delivered',
    /** Buffered waiting for recipient to connect. */
    buffered = 'buffered',
}

export interface SendOptions {
    /** How many seconds to wait for delivery. */
    wait?: number
    /**
     * If set will guarantee delivery to at least one subscriber,
     * otherwise throw an error.
     */
    requireDelivery?: boolean
}

export interface SendContext {
    /** May be set by broker to allow sender to abort an ongoing send. */
    cancel?: () => void
}

/** Can be called to cancel a subscription. */
export type Unsubscriber = () => void

/**
 * Function that is called when a subscription updates.
 * Should return a promise that resolves when the subscription has been updated.
 * May throw to indicate that the update failed.
 */
export type Updater = (payload: Buffer) => Promise<void>

export interface Broker {
    /** Send payload to channel and optionally wait for delivery.  */
    send(
        channel: string,
        payload: Buffer,
        options: SendOptions,
        ctx?: SendContext
    ): Promise<DeliveryState>
    /** Subscribe to payloads on a channel. */
    subscribe(channel: string, updater: Updater): Promise<Unsubscriber>
    /** Called during setup, can be used to initialize broker. */
    init(): Promise<void>
    /** Called during teardown, can be used to cleanup and connections. */
    deinit(): Promise<void>
    /** Perform a health check on the broker, should throw if there is a problem. */
    healthCheck(): Promise<void>
}
