import { Stat } from 'node-lmdb'
import * as prettyBytes from 'pretty-bytes'

export type TableStat = Stat & {
  totalPages: number
  totalSize: string
}

export function toTableStat(stat: Stat): TableStat {
  const totalPages =
    stat.treeBranchPageCount +
    stat.treeLeafPageCount +
    (stat.overflowPages || 0)
  const totalSize = totalPages * stat.pageSize
  return {
    ...stat,
    totalPages,
    totalSize: prettyBytes(totalSize),
  }
}
