import { mkdir } from 'fs/promises'
import { DbiOptions, Env, EnvOptions, Key } from 'node-lmdb'
import { Table } from './table'
import { toTableStat, TableStat } from './tableStat'
import { Transaction } from './transaction'

// This registry ensures that we don't attempt to open the same
// LMDB Env multiple times within the same process.
const dbs: Record<string, Database> = {}

export async function openDB(options: EnvOptions): Promise<Database> {
  const path = options.path
  if (dbs[path]) {
    dbs[path].close()
  }
  await mkdir(options.path, { recursive: true })
  const db = new Database(options)
  db.open()
  return db
}

export class Database {
  env: Env
  config: EnvOptions

  constructor(config: EnvOptions) {
    this.config = config
    this.env = new Env()
  }

  open(): void {
    this.env.open(this.config)
    // register this db
    dbs[this.config.path] = this
  }

  close(): void {
    this.env.close()
    // unregister this db
    delete dbs[this.config.path]
  }

  openTable<K extends Key = string>(options: DbiOptions): Table<K> {
    return new Table(this, options)
  }

  beginRead(): Transaction {
    return new Transaction(this)
  }

  beginWrite(): Transaction {
    return new Transaction(this, { writeable: true })
  }

  /**
   * Detach from the memory-mapped object retrieved with getStringUnsafe()
   * or getBinaryUnsafe(). This must be called after reading the object and
   * before it is accessed again, or V8 will crash.
   * @param buffer
   */
  detachBuffer(buffer: ArrayBufferLike): void {
    this.env.detachBuffer(buffer)
  }

  stat(): TableStat {
    return toTableStat(this.env.stat())
  }
}
