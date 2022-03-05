import { Database } from './database'
import { Table } from './table'

const DEFAULT_QUEUE_NAME = 'queue'
const DEFAULT_VISIBILITY_TIMEOUT_SECS = 360 // six minutes
const DEFAULT_MAX_RECEIVES = 3
const DEFAULT_MAX_RETENTION_HOURS = 72 // three days

export interface Message {
  id: number
  enqueued: number // timestamp
  received: number // timestamp
  numReceives: number // timestamp
  body: string
  dedupKey?: string
}

export interface QueueOptions {
  name: string
  dedup?: boolean
  requireAck?: boolean
  visibilityTimeoutSecs?: number
  maxReceives?: number
  maxRetentionHours?: number
}

type QueueConfig = Required<QueueOptions>

const defaultConfig: QueueConfig = {
  name: DEFAULT_QUEUE_NAME,
  dedup: true,
  requireAck: true,
  visibilityTimeoutSecs: DEFAULT_VISIBILITY_TIMEOUT_SECS,
  maxReceives: DEFAULT_MAX_RECEIVES,
  maxRetentionHours: DEFAULT_MAX_RETENTION_HOURS,
}

export class Queue {
  config: QueueConfig
  db: Database
  queue: Table<number>
  index?: Table<string>

  // table keys:
  // - positive integers: the queue
  // - negative integers: dead-letter queue

  constructor(db: Database, options: QueueOptions) {
    this.config = Object.assign({}, defaultConfig, options)
    this.db = db
    this.queue = db.openTable({
      name: options.name,
      create: true,
    })
    if (options.dedup) {
      this.index = db.openTable({
        name: `${options.name}_dedup`,
        create: true,
      })
    }
  }

  /** return false if duplicate already exists in queue */
  send(body: string, dedupKey?: string): boolean {
    const txn = this.db.beginWrite()
    // Deduplicate
    if (this.index) {
      if (dedupKey) {
        throw new Error('This queue does not support deduplication')
      }
      const key = dedupKey || body
      if (this.index.getString(key)) {
        return false
      }
    }
    // Find the last message in the queue
    const cursor = this.queue.cursor(txn)
    const key = cursor.goToLast() || 0
    cursor.close()
    // increment id
    const id = key + 1
    // Build Message
    const now = Date.now()
    const message: Message = {
      id,
      enqueued: now,
      received: 0,
      numReceives: 0,
      body,
      dedupKey,
    }
    // Put message into queue
    this.queue.putString(id, JSON.stringify(message), txn)
    // Put entry into dedup index
    if (this.index) {
      const key = dedupKey || body
      // TODO: perform encoding magic on dedupKey
      this.index.putNumber(key, now, txn)
    }
    // Commit transaction
    txn.commit()
    return true
  }

  receive(): Message | null {
    const txn = this.db.beginWrite()

    // Find the earliest message in the queue
    const cursor = this.queue.cursor(txn)
    let key = cursor.goToRange(0)
    if (!key) {
      cursor.close()
      txn.abort()
      return null
    }

    const now = Date.now()
    let message: Message = JSON.parse(cursor.getCurrentString() as string)

    const dedupKey = message.dedupKey || message.body

    if (!this.config.requireAck) {
      if (this.index) {
        this.index.del(dedupKey, txn)
      }
      this.queue.del(message.id, txn)
      message.numReceives++
      message.received = now
      cursor.close()
      txn.commit()
      return message
    }

    let valid = false
    while (!valid) {
      if (!message.received) {
        valid = true
      } else if (
        now - message.received >
        this.config.visibilityTimeoutSecs * 1000
      ) {
        // If dead, send to DLQ
        if (message.numReceives >= this.config.maxReceives) {
          this.queue.del(message.id, txn)
          message.id = -message.id
          this.queue.putString(message.id, JSON.stringify(message), txn)
        } else {
          valid = true
        }
      }
      // If not visible, skip to next message
      key = cursor.goToNext()
      if (!key) {
        // No valid messages in queue
        cursor.close()
        txn.commit()
        return null
      }
      message = JSON.parse(cursor.getCurrentString() as string)
    }
    cursor.close()

    // update message metadata
    message.numReceives++
    message.received = now

    // If not requireAck, delete from queue and index
    if (!this.config.requireAck) {
      if (this.index) {
        this.index.del(dedupKey, txn)
      }
      this.queue.del(message.id, txn)
    } else {
      // Otherwise, update message metadata
      this.queue.putString(message.id, JSON.stringify(message), txn)
    }

    // Commit transaction and return message
    txn.commit()
    return message
  }

  private sweepDeadMessages(): void {
    throw new Error('not implemented')
  }

  receiveDeadletter(): Message {
    throw new Error('not implemented')
    // begin transaction
    // find earliest message
    // if dead, delete
  }

  ack(message: Message): void {
    // begin transaction
    const txn = this.db.beginWrite()
    // delete from queue
    this.queue.del(message.id, txn)
    // delete from index
    if (this.index) {
      this.index.del(message.dedupKey || message.body, txn)
    }
    // commit transaction
    txn.commit()
  }

  purge(): void {
    throw new Error('not implemented')
    // Should this use the batch API?
  }

  purgeDeadletter(): void {
    throw new Error('not implemented')
    // Should this use the batch API?
  }

  redriveDeadletter(): void {
    throw new Error('not implemented')
    // Should this use the batch API?
  }
}
