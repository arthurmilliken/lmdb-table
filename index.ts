import { Database, openDB } from './src/database'

export const main = async () => {
  const db = await openDB({
    path: '.testdata',
  })
  console.log(db.stat())
}

main()
