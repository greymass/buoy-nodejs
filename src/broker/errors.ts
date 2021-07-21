/** Thrown when a delivery error or timeout occurs. */
export class DeliveryError extends Error {
    code = 'E_DELIVERY'
    constructor(readonly reason: string) {
        super(reason)
    }
}

/** Thrown when a send is canceled. */
export class CancelError extends Error {
    code = 'E_CANCEL'
    constructor(readonly reason: string = 'Cancelled') {
        super(reason)
    }
}
