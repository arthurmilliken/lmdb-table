import { Dbi, DbiOptions, Key, KeyType, Cursor, PutOptions } from 'node-lmdb'
import { Database } from './database'
import { Transaction } from './transaction'

import { TableStat, toTableStat } from './tableStat'

export class Table<K extends Key = string> {
  db: Database
  dbi: Dbi
  config: DbiOptions

  constructor(db: Database, options: DbiOptions) {
    this.db = db
    this.dbi = db.env.openDbi(options)
    this.config = options
  }

  beginRead(): Transaction {
    return this.db.beginRead()
  }

  beginWrite(): Transaction {
    return this.db.beginWrite()
  }

  cursor(txn: Transaction): Cursor<K> {
    return new Cursor(txn.txn, this.dbi)
  }

  hasKey(key: K, txn?: Transaction): boolean {
    return this.useTransaction((useTxn) => {
      const cur = this.cursor(useTxn)
      const result = cur.goToKey(key) ? true : false
      cur.close()
      return result
    }, txn)
  }

  del(key: K, txn?: Transaction): void {
    this.useTransaction(
      (useTxn) => {
        useTxn.del(this, key)
      },
      txn,
      { writeable: true }
    )
  }

  getString(key: K, txn?: Transaction, options?: KeyType): string {
    return this.useTransaction((useTxn) => {
      return useTxn.getString(this, key, options)
    }, txn)
  }

  /**
   * Retrieve string with zero-copy semantics. Don't forget to call
   * Database.detach() on the underlying ArrayBuffer after you access
   * this data!
   */
  getStringUnsafe(key: K, txn?: Transaction, options?: KeyType): string {
    return this.useTransaction((useTxn) => {
      return useTxn.getStringUnsafe(this, key, options)
    }, txn)
  }

  putString(
    key: K,
    value: string,
    txn?: Transaction,
    options?: PutOptions
  ): void {
    this.useTransaction(
      (useTxn) => {
        useTxn.putString(this, key, value, options)
      },
      txn,
      { writeable: true }
    )
  }

  getBinary(key: K, txn?: Transaction, options?: KeyType): Buffer {
    return this.useTransaction((useTxn) => {
      return useTxn.getBinary(this, key, options)
    }, txn)
  }

  /**
   * Retrieve Buffer with zero-copy semantics. Don't forget to call
   * Database.detach() on the underlying ArrayBuffer after you access
   * this data!
   */
  getBinaryUnsafe(key: K, txn?: Transaction, options?: KeyType): Buffer {
    return this.useTransaction((useTxn) => {
      return useTxn.getBinaryUnsafe(this, key, options)
    }, txn)
  }

  putBinary(
    key: K,
    value: Buffer,
    txn?: Transaction,
    options?: PutOptions
  ): void {
    this.useTransaction(
      (useTxn) => {
        useTxn.putBinary(this, key, value, options)
      },
      txn,
      { writeable: true }
    )
  }

  getNumber(key: K, txn?: Transaction, options?: KeyType): number {
    return this.useTransaction((useTxn) => {
      return useTxn.getNumber(this, key, options)
    }, txn)
  }

  putNumber(
    key: K,
    value: number,
    txn?: Transaction,
    options?: PutOptions
  ): void {
    this.useTransaction(
      (useTxn) => {
        useTxn.putNumber(this, key, value, options)
      },
      txn,
      { writeable: true }
    )
  }

  getBoolean(key: K, txn?: Transaction, options?: KeyType): boolean {
    return this.useTransaction((useTxn) => {
      return useTxn.getBoolean(this, key, options)
    }, txn)
  }

  putBoolean(
    key: K,
    value: boolean,
    txn?: Transaction,
    options?: PutOptions
  ): void {
    this.useTransaction(
      (useTxn) => {
        useTxn.putBoolean(this, key, value, options)
      },
      txn,
      { writeable: true }
    )
  }

  close(): void {
    this.dbi.close()
  }

  drop(): void {
    this.dbi.drop()
  }

  stat(txn?: Transaction): TableStat {
    return this.useTransaction((useTxn) => {
      return toTableStat(this.dbi.stat(useTxn.txn))
    }, txn)
  }

  /**
   * Helper method for executing transactional code. If the user does not
   * supply a transaction argument, the helper will create a temporary
   * transaction, pass the temporary transaction into the callback, then
   * commit the temporary transaction when the callback is complete.
   *
   * If the user does supplies a transaction, then that transaction is
   * passed back into the callback, but it is up to the user to commit or
   * abort.
   *
   * @template T
   * @param callback
   * @param txn
   * @param options
   * @returns {T}
   *
   * @example
   * // Create a temporary read-only transaction and automatically abort
   * const stat: Stat = this.useTransaction((useTxn) => {
   *   return this.dbi.stat(useTxn)
   * })
   *
   * @example
   * // Create a temporary read-write transaction and automatically commit
   * const stat: Stat = this.useTransaction((useTxn) => {
   *   useTxn.putString('KEY_01', 'some value')
   *   return this.dbi.stat(useTxn)
   * }, null, { writeable: true })
   *
   * @example
   * // Use a pre-existing transaction, which must be committed manually
   * const myTxn = new Transaction(this.env, { writeable: true })
   * this.useTransaction((useTxn) => {
   *   // ...perform business logic
   * }, myTxn)
   * if (!myTxn.isClosed) myTxn.commit()
   */
  protected useTransaction<T>(
    callback: (useTxn: Transaction) => T,
    txn?: Transaction,
    options?: { writeable: boolean }
  ): T {
    // Create a new transaction (if necessary)
    let useTxn: Transaction
    if (txn) useTxn = txn
    else useTxn = new Transaction(this.db, options)

    let result
    try {
      result = callback(useTxn)
      // Close the new transaction (if created)
      if (!txn && !useTxn.isClosed) {
        if (useTxn.isWriteable) useTxn.commit()
        else useTxn.abort()
      }
      return result
    } catch (err) {
      if (!txn && !useTxn.isClosed) {
        useTxn.abort()
      }
      throw err
    }
  }
}
