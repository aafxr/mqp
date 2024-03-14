import * as amqp from 'amqplib/callback_api'
import {EventEmitter} from "stream";


export class Publish extends EventEmitter {
    static CONNECTION_ERROR_EVENT = 'connection-error'
    static CONNECTED_EVENT = 'connected'
    static CLOSE_EVENT = 'close'

    host: string
    port: string
    exchange: string
    errorMessage = ''

    connection: amqp.Connection | undefined
    channel: amqp.Channel | undefined

    private _connected = false
    private _reconnectTime = 2000


    constructor(exchange: string, host: string, port: string) {
        super()
        this.host = host
        this.port = port
        this.exchange = exchange
        this.connectionError = this.connectionError.bind(this)
    }

    async connect(): Promise<boolean> {
        if (this.channel) this.channel.close(this.connectionError)
        if (this.connection) this.connection.close()

        console.log('Publisher try connect')

        return await new Promise((resolve) => {
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
                    channel.assertExchange(this.exchange, "direct", {durable: false});
                    this.channel = channel
                    this._connected = true
                    console.log('Publisher connected')
                    this.emit(Publish.CONNECTED_EVENT)
                    resolve(true)
                })
            })
        })
    }


    private connectionError(err: Error) {
        console.error(err)
        this.emit(Publish.CONNECTION_ERROR_EVENT, err)
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


    send<T>(message: T, queue: string): boolean {
        if (!this.connection || !this.channel) {
            this.errorMessage = "AMQP chanel not init"
            return false
        }
        let msg = typeof message === 'string' ? message : JSON.stringify(message)
        try {
            return this.channel.publish(this.exchange, queue, Buffer.from(msg));
        } catch (err) {
            this.connectionError(<Error>err)
            return false
        }
    }


    close() {
        return new Promise((resolve, reject) => {

            if (this.channel) this.channel.close(reject)
            if (this.connection) this.connection.close()
            this.channel = undefined
            this.connection = undefined
            this.emit(Publish.CLOSE_EVENT)
            resolve(true)
        })
    }
}

