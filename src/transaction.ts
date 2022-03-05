import { Txn, Key, KeyType, PutOptions } from 'node-lmdb'
import { Database } from './database'
import { Table } from './table'

const MAX_KEY_SIZE = 511

function assertKeySize<K extends Key>(key: K): void {
  if (key instanceof Buffer) {
    if (key.length > MAX_KEY_SIZE)
      throw new Error(
        `Key<Buffer> size exceeds maximum length of ${MAX_KEY_SIZE}`
      )
  } else if (typeof key === 'string') {
    if (key.length * 2 > MAX_KEY_SIZE)
      throw new Error(
        `Key<string> size exceeds maximum length of ${Math.floor(
          MAX_KEY_SIZE / 2
        )}`
      )
  }
}

export class Transaction {
  lmdbEnv: Database
  txn: Txn
  isWriteable: boolean
  isClosed: boolean
  isReset: boolean

  constructor(lmdbEnv: Database, options?: { writeable: boolean }) {
    this.lmdbEnv = lmdbEnv
    this.txn = lmdbEnv.env.beginTxn({ readOnly: !options?.writeable })
    this.isClosed = false
    this.isReset = false
    this.isWriteable = options?.writeable || false
  }

  private assertRead<K extends Key>(key: K): void {
    assertKeySize(key)
    if (this.isReset || this.isClosed) {
      throw new Error('The transaction is already closed.')
    }
  }

  private assertWrite<K extends Key>(key: K): void {
    this.assertRead(key)
    if (!this.isWriteable) {
      throw new Error('The transaction is read-only.')
    }
  }

  getString<K extends Key = string>(
    table: Table<K>,
    key: K,
    options?: KeyType
  ): string {
    this.assertRead(key)
    return this.txn.getString(table.dbi, key, options)
  }

  putString<K extends Key = string>(
    table: Table<K>,
    key: K,
    value: string,
    options?: PutOptions
  ): void {
    this.assertWrite(key)
    this.txn.putString(table.dbi, key, value, options)
  }

  getBinary<K extends Key = string>(
    table: Table<K>,
    key: K,
    options?: KeyType
  ): Buffer {
    this.assertRead(key)
    return this.txn.getBinary(table.dbi, key, options)
  }

  putBinary<K extends Key = string>(
    table: Table<K>,
    key: K,
    value: Buffer,
    options?: PutOptions
  ): void {
    this.assertWrite(key)
    this.txn.putBinary(table.dbi, key, value, options)
  }

  getNumber<K extends Key = string>(
    table: Table<K>,
    key: K,
    options?: KeyType
  ): number {
    this.assertRead(key)
    return this.txn.getNumber(table.dbi, key, options)
  }

  putNumber<K extends Key = string>(
    table: Table<K>,
    key: K,
    value: number,
    options?: PutOptions
  ): void {
    this.assertWrite(key)
    this.txn.putNumber(table.dbi, key, value, options)
  }

  getBoolean<K extends Key = string>(
    table: Table<K>,
    key: K,
    options?: KeyType
  ): boolean {
    this.assertRead(key)
    return this.txn.getBoolean(table.dbi, key, options)
  }

  putBoolean<K extends Key = string>(
    table: Table<K>,
    key: K,
    value: boolean,
    options?: PutOptions
  ): void {
    this.assertWrite(key)
    this.txn.putBoolean(table.dbi, key, value, options)
  }

  del<K extends Key = string>(
    table: Table<K>,
    key: K,
    options?: KeyType
  ): void {
    this.assertWrite(key)
    this.txn.del(table.dbi, key, options)
  }

  getStringUnsafe<K extends Key = string>(
    table: Table<K>,
    key: K,
    options?: KeyType
  ): string {
    this.assertRead(key)
    return this.txn.getStringUnsafe(table.dbi, key, options)
  }

  getBinaryUnsafe<K extends Key = string>(
    table: Table<K>,
    key: K,
    options?: KeyType
  ): Buffer {
    this.assertRead(key)
    return this.txn.getBinaryUnsafe(table.dbi, key, options)
  }

  commit(): void {
    this.txn.commit()
    this.isClosed = true
  }

  abort(): void {
    this.txn.abort()
    this.isClosed = true
  }

  reset(): void {
    this.txn.reset()
    this.isReset = true
  }

  renew(): void {
    this.txn.renew()
    this.isReset = false
  }
}
