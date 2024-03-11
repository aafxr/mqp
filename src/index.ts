import "dotenv/config";

import { MessageType } from "./types/MessageType";
import { Server, Socket } from "socket.io";
import { Rabbit } from "./class";
import { Publish } from "./class/Publish";
import { Consume } from "./class/Consume";

const socket_hostname = process.env.SOCKET_HOST as string;
const rabitmq_hostname = process.env.RABITMQ_HOST as string;
const socket_port = process.env.SOCKET_PORT as string;
const rabbit_port = process.env.RABBIT_PORT as string;


async function socket_init(){
    const io = new Server(Number(socket_port),{ 
        cors: {
            origin: `*`,
            methods: ["GET", "POST"]
        }
    });
    
    
    
    let clients:Record<string, Socket> = {};
    
    let queues: Record<string, Consume> = {}
    
    const travelPublisher = new Publish('travel', rabitmq_hostname, rabbit_port)
    
    let travelPublisherReady = await travelPublisher.init().catch(console.error) 
    
    
    async function createNewQueue(queueName: string, userID?:string, socket?: Socket){
        queues[queueName] = new Consume('travel', queueName, rabitmq_hostname, rabbit_port)
        if(userID && socket) queues[queueName].join(userID, socket)
        await queues[queueName].connect()
    }
    
    
    io.on("connection", (socket) => {
        clients[socket.id] = socket
    
        socket.on("join", async (msg: MessageType) => {
            try {
                if ('join' in msg) {
                    const travelID = msg.join?.travelID
                    if(!travelID) return 
                    await createNewQueue(travelID, socket.id, socket)
                }
            } catch (error) {
                console.log((<Error>error).message)
            }
    
        })
        
        
        socket.on("leave", (msg: MessageType) => {
            try {
                if('leave' in msg){
                    const travelID = msg.leave?.travelID
                    if(!travelID) return 
                    const q = queues[travelID]
                    if(!q) return 
                    q.leave(socket.id)
                    if(q.usersCount() === 0) {
                        q.close()
                        delete queues[travelID]
                    }
                }
            } catch (error) {
                console.log((<Error>error).message)
            }
        })
    
        
        socket.on("message", async (msg: MessageType) => {
            try {
                if (!travelPublisherReady) {
                    console.log('Publisher not ready');
                    
                    return 
                }
                if('message' in msg){
                    const travelID = msg.message?.primary_entity_id
                    if(travelID) {
                        const sending = travelPublisher.send(msg, travelID)
                        console.log(`${sending ? '[x] Done, ': '[ ] Fail, '}${JSON.stringify(msg)}`)
                        console.log(travelPublisher.errorMessage);
                        
                    }
                    return 
                }
            } catch (error) {
                console.log((<Error>error).message)
            }
        });
    
        socket.on("disconnect", () => {
            if(clients[socket.id] ) {
                delete clients[socket.id]
            }
        });
    });

    setInterval(() => {
        const users = Object.keys(clients)
        console.log('users count: ', users.length);
        const qq = Object.keys(queues)
        console.log('Queues count: ', qq.length)
        console.log(qq);
        
    }, 10 * 1000)

}


socket_init()

