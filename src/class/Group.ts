export class Group<T>{
    group: Map<string, Set<T>>

    constructor(){
        this.group = new Map()
    }

    add(groupName: string, item: T){
        if(!this.group.has(groupName)) this.group.set(groupName, new Set())
        this.group.get(groupName)?.add(item)
    }

    addToAll(item: T){
        const keyIterator = this.group.keys()
        for(const key of keyIterator){
            this.group.get(key)?.add(item)
        }
    }

    delete(groupName: string, item:T){
        this.group.get(groupName)?.delete(item)
    }

    deleteFromAllGroups(item: T){
        const keyIterator = this.group.keys()
        for(const key of keyIterator){
            const g = this.group.get(key)
            g?.delete(item)
        }
    }

    clearGroup(groupName:string){
        const g = this.group.get(groupName)
        g?.clear()
    }

    clearAll(){
        const keyIterator = this.group.keys()
        for(const key of keyIterator){
            this.group.get(key)?.clear()
        }
    }

    isInGroup(groupName: string, item: T){
        return Boolean(this.group.get(groupName)?.has(item))
    }

    getItems(groupName:string){
        const result =  this.group.get(groupName)?.values()
        return result ? [...result] : result
    }

    hasGroup(groupName: string){
        return this.group.has(groupName)
    }

}
