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

/** Can be called to cancel a subscription. */
export type Unsubscriber = () => void

/** Function that is called when a subscription updates. */
export type Updater = (payload: Buffer) => void

export interface Broker {
    /** Send payload to channel and optionally wait for delivery.  */
    send(channel: string, payload: Buffer, options: SendOptions): Promise<DeliveryState>
    /** Subscribe to payloads on a channel. */
    subscribe(channel: string, updater: Updater): Promise<Unsubscriber>
}
