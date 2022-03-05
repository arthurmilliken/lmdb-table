import { Stat } from 'node-lmdb'
import { TableStat, toTableStat } from '../tableStat'

describe('toTableStat', () => {
  it('correctly calculates totalPages and totalSize', () => {
    const stat: Stat = {
      pageSize: 4096,
      treeDepth: 2,
      treeBranchPageCount: 1,
      treeLeafPageCount: 15,
      entryCount: 1000,
      overflowPages: 2000,
    }
    const expected: TableStat = {
      ...stat,
      totalPages: 2016,
      totalSize: '8.26 MB',
    }
    expect(toTableStat(stat)).toStrictEqual(expected)
  })
})
