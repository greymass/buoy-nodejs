import config from 'config'
import * as cluster from 'cluster'
import * as http from 'http'
import * as os from 'os'
import * as WebSocket from 'ws'
import {URL} from 'url'

import logger from './logger'
import version from './version'
import setupBroker, {Broker, SendContext} from './broker'

let broker: Broker

const HEARTBEAT_INTERVAL = 10 * 1000

/** A WebSocket connection. */
class Connection {
    static seq = 0

    alive: boolean
    closed: boolean
    id: number

    private cleanupCallback: () => void
    private socket: WebSocket
    private timer: NodeJS.Timeout
    private log: typeof logger

    constructor(socket: WebSocket, cleanup: () => void) {
        this.id = ++Connection.seq
        this.log = logger.child({conn: this.id})
        this.socket = socket
        this.closed = false
        this.alive = true
        this.cleanupCallback = cleanup
        this.socket.on('close', () => {
            this.didClose()
        })
        this.socket.on('pong', () => {
            this.alive = true
        })
        this.socket.on('error', (error) => {
            this.log.warn(error, 'socket error')
        })
        this.timer = setInterval(() => {
            if (this.alive) {
                this.alive = false
                this.socket.ping()
            } else {
                this.destroy()
            }
        }, HEARTBEAT_INTERVAL)
    }

    private didClose() {
        this.log.debug({alive: this.alive, closed: this.closed}, 'did close')
        this.alive = false
        clearTimeout(this.timer)
        if (this.closed === false) {
            this.cleanupCallback()
        }
        this.closed = true
    }

    send(data: Buffer) {
        this.log.debug({size: data.byteLength}, 'send data')
        this.socket.send(data)
    }

    close(code?: number, reason?: string) {
        this.socket.close(code, reason)
        this.didClose()
    }

    destroy() {
        this.socket.terminate()
        this.didClose()
    }
}

function getUUID(request: http.IncomingMessage) {
    const url = new URL(request.url || '', 'http://localhost')
    const uuid = url.pathname.slice(1)
    if (uuid.length < 10) {
        throw new HttpError('Invalid channel name', 400)
    }
    return uuid
}

async function handleConnection(socket: WebSocket, request: http.IncomingMessage) {
    const uuid = getUUID(request)
    const connection = new Connection(socket, () => {
        log.info('connection closed')
        unsubscribe()
    })
    const log = logger.child({uuid, conn: connection.id})
    const unsubscribe = await broker.subscribe(uuid, (data) => {
        connection.send(data)
    })
}

class HttpError extends Error {
    statusCode: number
    constructor(message: string, statusCode: number) {
        super(message)
        this.statusCode = statusCode
    }
}

async function handlePost(request: http.IncomingMessage, response: http.ServerResponse) {
    const uuid = getUUID(request)
    const log = logger.child({uuid})
    const data = await readBody(request)
    if (data.byteLength === 0) {
        throw new HttpError('Unable to forward empty message', 400)
    }
    const ctx: SendContext = {}
    request.once('close', () => {
        if (ctx.cancel) {
            ctx.cancel()
        }
    })
    const waitHeader = request.headers['x-buoy-wait'] || request.headers['x-buoy-soft-wait']
    const requireDelivery = !!request.headers['x-buoy-wait']
    let wait = 0
    if (waitHeader) {
        wait = Math.min(Number(waitHeader), 120)
        if (!Number.isFinite(wait)) {
            throw new HttpError('Invalid wait timeout', 400)
        }
    }
    try {
        const delivery = await broker.send(uuid, data, {wait, requireDelivery}, ctx)
        response.setHeader('X-Buoy-Delivery', delivery)
        log.info({delivery}, 'message dispatched')
    } catch (error) {
        if (error.code === 'E_CANCEL') {
            throw new HttpError('Request cancelled', 410)
        } else if (error.code === 'E_TIMEOUT') {
            throw new HttpError('Timed out waiting for connection', 408)
        } else {
            throw error
        }
    }
}

function readBody(request: http.IncomingMessage) {
    return new Promise<Buffer>((resolve, reject) => {
        const chunks: Buffer[] = []
        request.on('error', reject)
        request.on('data', (chunk) => {
            chunks.push(chunk)
        })
        request.on('end', () => {
            resolve(Buffer.concat(chunks))
        })
    })
}

function handleRequest(request: http.IncomingMessage, response: http.ServerResponse) {
    response.setHeader('Server', `buoy/${version}`)
    response.setHeader('Access-Control-Allow-Origin', '*')
    response.setHeader('Access-Control-Allow-Headers', '*')
    response.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS')
    response.setHeader('Access-Control-Expose-Headers', 'X-Buoy-Delivery')
    if (request.method !== 'POST') {
        response.setHeader('Allow', 'POST, OPTIONS')
        response.statusCode = request.method === 'OPTIONS' ? 200 : 405
        response.end()
        return
    }
    if (request.url === '/test') {
        response.statusCode = 200
        response.write('Ok')
        response.end()
        return
    }
    if (request.url === '/health_check') {
        broker
            .healthCheck()
            .then(() => {
                response.statusCode = 200
                response.write('Ok')
                response.end()
            })
            .catch((error) => {
                response.statusCode = 500
                response.write(error.message || String(error))
                response.end()
            })
        return
    }
    handlePost(request, response)
        .then(() => {
            response.statusCode = 200
            response.write('Ok')
            response.end()
        })
        .catch((error) => {
            if (error instanceof HttpError) {
                logger.info(error, 'error handling post request')
                response.statusCode = error.statusCode
                response.write(error.message)
            } else {
                logger.error(error, 'unexpected error handling post request')
                response.statusCode = 500
                response.write('Internal server error')
            }
            response.end()
        })
}

async function setup(port: number) {
    const httpServer = http.createServer(handleRequest)
    const websocketServer = new WebSocket.Server({server: httpServer})
    broker = await setupBroker()
    await new Promise<void>((resolve, reject) => {
        httpServer.listen(port, resolve)
        httpServer.once('error', reject)
    })

    websocketServer.on('connection', (socket, request) => {
        handleConnection(socket as any, request).catch((error) => {
            logger.error(error, 'error handling websocket connection')
            socket.close()
        })
    })

    return () =>
        new Promise<void>((resolve, reject) => {
            httpServer.close((error) => {
                if (error) {
                    reject(error)
                } else {
                    resolve()
                }
            })
        })
}

export async function main() {
    const port = Number.parseInt(config.get('port'))
    if (!Number.isFinite(port)) {
        throw new Error('Invalid port number')
    }
    if (cluster.isMaster) {
        logger.info({version}, 'starting')
    }
    let numWorkers = Number.parseInt(config.get('num_workers'), 10)
    if (numWorkers === 0) {
        numWorkers = os.cpus().length
    }
    const isMaster = cluster.isMaster && numWorkers > 1
    let teardown: () => Promise<void> | undefined
    if (isMaster) {
        logger.info('spawning %d workers', numWorkers)
        const runningPromises: Promise<void>[] = []
        for (let i = 0; i < numWorkers; i++) {
            const worker = cluster.fork()
            const running = new Promise<void>((resolve, reject) => {
                worker.once('message', (message) => {
                    if (message.error) {
                        reject(new Error(message.error))
                    } else {
                        resolve()
                    }
                })
            })
            runningPromises.push(running)
        }
        await Promise.all(runningPromises)
        logger.info({port}, 'server running')
    } else {
        try {
            teardown = await setup(port)
            if (process.send) {
                process.send({ready: true})
            }
        } catch (error) {
            if (process.send) {
                process.send({error: error.message || String(error)})
            }
            throw error
        }
    }

    async function exit() {
        if (teardown) {
            await teardown()
        }
        return 0
    }

    process.on('SIGTERM', () => {
        if (cluster.isMaster) {
            logger.info('got SIGTERM, exiting...')
        }
        exit()
            .then((code) => {
                logger.debug('bye')
                process.exit(code)
            })
            .catch((error) => {
                logger.fatal(error, 'unable to exit gracefully')
                setTimeout(() => process.exit(1), 1000)
            })
    })
}

if (module === require.main) {
    process.once('uncaughtException', (error) => {
        logger.error(error, 'Uncaught exception')
        abort(1)
    })
    main().catch((error) => {
        if (cluster.isMaster) {
            logger.fatal(error, 'Unable to start application')
        }
        abort(1)
    })
}

function abort(code: number) {
    process.exitCode = code
    setImmediate(() => {
        process.exit(code)
    })
}
