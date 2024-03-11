import * as amqp from "amqplib/callback_api";
import { Socket } from "socket.io";

export class Rabbit {
    connection: amqp.Connection | undefined
    channel: amqp.Channel | undefined
    queue: amqp.Replies.AssertQueue | undefined

    exchangeName = ''
    queueName = ''

    users: Record<string, Socket> = {}

    host: string
    port: string

    constructor(exchange: string, queue: string, host: string, port: string ){
        this.exchangeName = exchange
        this.queueName = queue
        this.host = host
        this.port = port 
        this.onMessage = this.onMessage.bind(this)
    }


    connect(exchange: string, queue: string): Promise<Rabbit> {
        this.exchangeName = exchange
        return new Promise((resolve, reject) => {
            amqp.connect(`amqp://${this.host}:${this.port}`, (error0, connection) => {
                if (error0) throw error0;
                this.connection = connection

                connection.createChannel((error1, channel) => {
                    if (error1)  throw error1;
                    this.channel = channel

                    channel.prefetch(1)

                    channel.assertQueue(queue, {  }, (error3, queue) => {
                        if(error3) throw error3
                        channel.bindQueue(queue.queue, this.exchangeName, this.queueName)
                        this.queue = queue
                        channel.consume(queue.queue, this.onMessage)
                        resolve(this)
                    })
                }, );
            });
        })
    }

    
    send<T>( message: T, exchange: string, queue:string):Promise<boolean>{
        return new Promise((resolve, reject) => {
            amqp.connect(`amqp://${this.host}:${this.port}`, (error0, connection)=>{
                if(error0) reject(error0)
                connection.createChannel((error1, channel) => {
                    if(error1) reject(error1)
                    let msg = typeof message === 'string' ? message : JSON.stringify(message) 
                    channel.assertExchange(exchange, "direct", { durable: false });
                    const result = channel.publish(exchange, queue, Buffer.from(msg));
                    channel.close(reject)
                    connection.close()
                    resolve(result)
                })
            })
        })
    }


    onMessage(msg:amqp.Message | null){
        
        if(!msg) return 
        const message = msg.content.toString()
        console.log('rmq -> ', message, Object.values(this.users).length);
        Object.values(this.users).forEach(s => s.emit('message', message))
        // this.channel?.ack(msg)
    }


    join(userID: string, socket: Socket){
        this.users[userID] = socket
    }


    leave(userID:string){
        if(this.users[userID])
            delete this.users[userID]
    }


    closeConnection(cb: (err: Error) => unknown){
        if(this.channel) {
            this.channel.close(cb || console.error)
            this.channel = undefined
        }
        if(this.connection){
            this.connection.close()
            this.connection = undefined
        }
    }
}
