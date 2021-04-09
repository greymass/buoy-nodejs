import {XXHash3} from 'xxhash-addon'

export class Hash {
    constructor(readonly bytes: Buffer) {}

    equals(other: Hash) {
        return this.bytes.equals(other.bytes)
    }

    toString() {
        return this.bytes.toString('hex')
    }

    toJSON() {
        return this.toString()
    }
}

export class Hasher {
    private hasher = new XXHash3()

    hash(data: Buffer): Hash {
        this.hasher.update(data)
        const rv = this.hasher.digest()
        this.hasher.reset()
        return new Hash(rv)
    }
}
