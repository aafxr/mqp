import * as amqp from 'amqplib/callback_api'
import {EventEmitter} from 'stream'


/**
 * доступные события:
 * - 'ready'
 * - 'error'
 * - 'message'
 * - 'close'
 * - 'connection-error'
 */
export class Consume extends EventEmitter {
    static READY_EVENT = 'ready'
    static ERROR_EVENT = 'error'
    static MESSAGE_EVENT = 'mwssage'
    static CLOSE_EVENT = 'close'
    static CONNECTION_ERROR_EVENT = 'connection-error'

    connection: amqp.Connection | undefined
    channel: amqp.Channel | undefined
    queue: amqp.Replies.AssertQueue | undefined

    exchangeName = ''
    queueName = ''

    host: string
    port: string

    private _reconnectTime = 2000
    private _connected = false


    constructor(exchange: string, queue: string, host: string, port: string) {
        super()
        this.exchangeName = exchange
        this.queueName = queue
        this.host = host
        this.port = port
        this.onMessage = this.onMessage.bind(this)
        this.connectionError = this.connectionError.bind(this)
    }


    connect(): Promise<boolean> {
        if (this.channel) this.channel.close(this.connectionError)
        if (this.connection) this.connection.close()

        console.log(`Consume ${this.queueName} try connect`)

        return new Promise((resolve, reject) => {
            amqp.connect(`amqp://${this.host}:${this.port}`, (error0, connection) => {
                if (error0) {
                    this.connectionError(error0)
                    resolve(false)
                    return
                }
                this.connection = connection

                connection.createChannel((error1, channel) => {
                    if (error1) {
                        this.connectionError(error1)
                        resolve(false)
                        return
                    }
                    this.channel = channel

                    channel.prefetch(1)

                    channel.assertQueue(this.queueName, {}, (error3, queue) => {
                        if (error3) {
                            this.connectionError(error1)
                            resolve(false)
                            return
                        }
                        channel.bindQueue(queue.queue, this.exchangeName, this.queueName)
                        this.queue = queue
                        this._connected = true
                        console.log(`Consume ${this.queueName} connected`)
                        channel.consume(queue.queue, this.onMessage)
                        this.emit(Consume.READY_EVENT)
                        resolve(true)
                    })
                },);
            });
        })
    }


    private connectionError(err: Error) {
        console.error(err)
        this.emit(Consume.CONNECTION_ERROR_EVENT, err)
        this._connected = false
        this.reconnect()
    }


    private reconnect(){
        if(this._connected){
            const timerID = setTimeout(async () => {
                await this.connect()
                if(!this._connected) this.reconnect()
                clearTimeout(timerID)
            }, this._reconnectTime)
        }
    }


    onMessage(msg: amqp.Message | null) {
        if (!msg) return
        const message = msg.content.toString()
        this.emit(Consume.MESSAGE_EVENT, message)
        this.channel?.ack(msg)
    }


    close() {
        return new Promise((resolve, reject) => {
            if (this.channel) {
                this.channel.close(reject)
                this.channel = undefined
            }
            if (this.connection) {
                this.connection.close()
                this.connection = undefined
            }
            this.emit(Consume.CLOSE_EVENT)
            resolve(true)
        })

    }


}