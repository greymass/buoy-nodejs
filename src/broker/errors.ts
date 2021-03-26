/** Thrown when a send timeout occurs. */
export class TimeoutError extends Error {
    code = 'E_TIMEOUT'
    constructor(timeout: number) {
        super(`Timed out after ${timeout}ms`)
    }
}

/** Thrown when a send is canceled. */
export class CancelError extends Error {
    code = 'E_CANCEL'
    constructor() {
        super('Cancelled')
    }
}
