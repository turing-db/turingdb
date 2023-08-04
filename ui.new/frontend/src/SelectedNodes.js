import { useState } from 'react'

export default class SelectedNodes {
    constructor(props) {
        this.nodes = props.nodes;
        this.setNodes = props.setNodes;
        this.ids = props.ids;
        this.setIds = props.setIds;
    }

    add(node) {
        this.setIds(prev => new Set([...prev, node.id]));
        this.setNodes(prev => [...prev, node]);
    }

    remove(node) {
        this.setIds(prev => {
            prev.delete(node.id);
            return new Set(prev);
        });
        this.setNodes(prev => prev.filter(n => node.id != n.id));
    }

    removeFromId(id) {
        this.setIds(prev => {
            prev.delete(id);
            return new Set(prev);
        });
        this.setNodes(prev => prev.filter(n => id != n.id));
    }

    clear() {
        this.setIds(new Set());
        this.setNodes([]);
    }

    has(n) {
        return this.ids.has(n.id);
    }

    hasId(id) {
        return this.ids.has(id);
    }
}
