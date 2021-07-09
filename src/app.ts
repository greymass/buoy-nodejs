import config from 'config'
import cluster, {Worker} from 'cluster'
import * as http from 'http'
import * as os from 'os'
import * as WebSocket from 'ws'
import {URL} from 'url'

import logger from './logger'
import version from './version'
import setupBroker, {Broker, SendContext, Unsubscriber} from './broker'
import type Logger from 'bunyan'

let broker: Broker
let requestSeq = 0
let activeRequests = 0
const connections: Connection[] = []

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
    let unsubscribe: Unsubscriber | null = null
    let prematureClose = false
    const connection = new Connection(socket, () => {
        log.debug('connection closed')
        if (unsubscribe) {
            unsubscribe()
        } else {
            prematureClose = true
        }
        connections.splice(connections.indexOf(connection), 1)
    })
    connections.push(connection)
    const log = logger.child({uuid, conn: connection.id})
    log.debug('new connection')
    unsubscribe = await broker.subscribe(uuid, (data) => {
        log.info('delivering payload')
        connection.send(data)
    })
    if (prematureClose) {
        unsubscribe()
    }
}

class HttpError extends Error {
    constructor(message: string, readonly statusCode: number) {
        super(message)
    }
}

async function handlePost(
    request: http.IncomingMessage,
    response: http.ServerResponse,
    log: Logger
) {
    const uuid = getUUID(request)
    log = log.child({uuid})
    const data = await readBody(request)
    if (data.byteLength === 0) {
        throw new HttpError('Unable to forward empty message', 400)
    }
    const ctx: SendContext = {}
    request.once('close', () => {
        response.end()
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
            throw new HttpError(`Request cancelled (${error.reason})`, 410)
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
    activeRequests++
    const log = logger.child({req: ++requestSeq})
    handlePost(request, response, log)
        .then(() => {
            response.statusCode = 200
            response.write('Ok')
            response.end()
        })
        .catch((error) => {
            if (response.writableEnded) {
                log.debug(error, 'error from ended request')
                return
            }
            if (error instanceof HttpError) {
                log.info('http %d when handling post request: %s', error.statusCode, error.message)
                response.statusCode = error.statusCode
                response.write(error.message)
            } else {
                log.error(error, 'unexpected error handling post request')
                response.statusCode = 500
                response.write('Internal server error')
            }
            response.end()
        })
        .finally(() => {
            activeRequests--
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

    return async () => {
        const close = new Promise<void>((resolve, reject) => {
            httpServer.close((error) => {
                if (error) {
                    reject(error)
                } else {
                    resolve()
                }
            })
        })
        connections.map((c) => c.close(1012, 'Shutting down'))
        await Promise.all([close, broker.deinit()])
    }
}

interface Status {
    activeConnections: number
    activeRequests: number
    numConnections: number
    numRequests: number
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
    let statusTimer: any
    const statusInterval = (Number(config.get('status_interval')) || 0) * 1000
    if (isMaster) {
        const workers: Worker[] = []
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
            workers.push(worker)
        }
        await Promise.all(runningPromises)
        if (statusInterval > 0) {
            const workerStatus: Record<number, Status> = {}
            workers.forEach((worker) => {
                worker.on('message', (message) => {
                    if (message.status) {
                        workerStatus[worker.id] = message.status
                    }
                })
            })
            statusTimer = setInterval(() => {
                const status: Status = {
                    activeConnections: 0,
                    activeRequests: 0,
                    numConnections: 0,
                    numRequests: 0,
                }
                for (const s of Object.values(workerStatus)) {
                    status.activeConnections += s.activeConnections
                    status.activeRequests += s.activeRequests
                    status.numConnections += s.numConnections
                    status.numRequests += s.numRequests
                }
                logger.info(status, 'status')
            }, statusInterval)
        }
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
        if (statusInterval > 0) {
            statusTimer = setInterval(() => {
                const status: Status = {
                    activeConnections: connections.length,
                    activeRequests,
                    numConnections: Connection.seq,
                    numRequests: requestSeq,
                }
                if (process.send) {
                    process.send({status})
                } else {
                    logger.info(status, 'status')
                }
            }, statusInterval)
        }
    }

    async function exit() {
        clearInterval(statusTimer)
        if (teardown) {
            const timeout = new Promise<never>((_, reject) => {
                setTimeout(() => {
                    reject(new Error('Timed out waiting for teardown'))
                }, 10 * 1000)
            })
            await Promise.race([teardown(), timeout])
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

    if (cluster.isMaster) {
        logger.info({port}, 'server running')
    }
}

if (module === require.main) {
    process.once('uncaughtException', (error) => {
        logger.error(error, 'Uncaught exception')
        if (cluster.isMaster) {
            abort(1)
        }
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
