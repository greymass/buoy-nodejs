import * as bunyan from 'bunyan'
import cluster from 'cluster'
import config from 'config'

let logger = bunyan.createLogger({
    name: config.get('name'),
    streams: (config.get('log') as any[]).map(({level, out}) => {
        if (out === 'stdout') {
            return {level, stream: process.stdout}
        } else if (out === 'stderr') {
            return {level, stream: process.stderr}
        } else {
            return {level, path: out}
        }
    }),
})

if (cluster.isWorker) {
    logger = logger.child({worker: cluster.worker!.id})
}

export default logger
