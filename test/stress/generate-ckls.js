import { readFile, writeFile, mkdir } from 'node:fs/promises'
import { existsSync } from 'node:fs'
import { resolve, dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { parseArgs } from 'node:util'

const __dirname = dirname(fileURLToPath(import.meta.url))
const TEMPLATE_PATH = join(__dirname, '..', 'e2e', 'testFiles', 'test.ckl')

const { values } = parseArgs({
  options: {
    count: { type: 'string', default: '20000' },
    'output-dir': { type: 'string', default: join(__dirname, 'ckl-files') }
  }
})

const count = parseInt(values.count, 10)
const outputDir = resolve(values['output-dir'])

async function generate () {
  const template = await readFile(TEMPLATE_PATH, 'utf8')

  if (!existsSync(outputDir)) {
    await mkdir(outputDir, { recursive: true })
  }

  const padWidth = String(count).length
  const startTime = Date.now()

  console.log(`Generating ${count} CKL files in ${outputDir}`)

  // Write in parallel batches to avoid overwhelming the filesystem
  const BATCH_SIZE = 500
  let written = 0

  for (let i = 0; i < count; i += BATCH_SIZE) {
    const batch = []
    const end = Math.min(i + BATCH_SIZE, count)
    for (let j = i; j < end; j++) {
      const hostName = `stress-host-${String(j + 1).padStart(padWidth, '0')}`
      const xml = template.replace(
        /<HOST_NAME>.*?<\/HOST_NAME>/is,
        `<HOST_NAME>${hostName}</HOST_NAME>`
      )
      const filePath = join(outputDir, `${hostName}.ckl`)
      batch.push(writeFile(filePath, xml, 'utf8'))
    }
    await Promise.all(batch)
    written += batch.length
    if (written % 5000 === 0 || written === count) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
      console.log(`  ${written}/${count} files (${elapsed}s)`)
    }
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log(`Done: ${count} files in ${elapsed}s`)
}

generate().catch(err => {
  console.error(err)
  process.exit(1)
})
