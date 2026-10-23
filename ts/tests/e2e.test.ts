/**
 * End-to-end smoke test.
 *
 * One realistic scenario that exercises the full stack:
 *
 *   Staged (default JSON codec) → VersionedKV → Keyset → Hamt → Memory
 *                       └─ Namespaced views                    └─ cleanOrphans
 *
 * Doubles as a runnable example: if you read one test to understand
 * what the API looks like in practice, read this one.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { type MergeFn, Namespaced, Staged, VersionedKV } from '../src/index'

interface Doc {
  text: string
  edits: number
}

describe('e2e: collaborative document store', () => {
  it('two writers concurrently edit a shared doc through Namespaced views', async () => {
    // ---------- Setup ----------
    const store = new Memory()

    // Open a Versioned at HEAD on `main`. Wrap with a Staged so writes
    // accumulate in memory until we commit.
    const vkAlice = await VersionedKV.open(store)
    const stagedAlice = new Staged(vkAlice)

    // Two namespaced views over the same Staged — Alice writes only
    // under `alice/*`, the shared doc lives under `docs/*`.
    const aliceProfile = new Namespaced(stagedAlice, 'alice')
    const docs = new Namespaced(stagedAlice, 'docs')

    // ---------- Alice's first commit ----------
    aliceProfile.set('name', 'Alice')
    aliceProfile.set('email', 'alice@example.com')
    docs.set('intro', { text: 'Welcome.', edits: 1 } satisfies Doc)

    expect(stagedAlice.hasChanges).toBe(true)

    const r1 = await stagedAlice.commit({ info: { author: 'alice', kind: 'init' } })
    expect(r1.merged).toBe(true)
    expect(r1.strategy).toBe('fast_forward')
    expect(stagedAlice.hasChanges).toBe(false)

    const initCommit = vkAlice.currentCommit
    expect(await vkAlice.commitInfo(initCommit)).toEqual({
      author: 'alice',
      kind: 'init',
    })

    // ---------- Read-back through namespaced views ----------
    expect(await aliceProfile.get<string>('name')).toBe('Alice')
    const intro = await docs.get<Doc>('intro')
    expect(intro?.text).toBe('Welcome.')
    expect(intro?.edits).toBe(1)

    // descendantKeys reaches across nested namespaces too.
    const allDocKeys: string[] = []
    for await (const k of docs.keys()) allDocKeys.push(k)
    expect(allDocKeys).toEqual(['intro'])

    // ---------- Bob opens a second handle on the same store ----------
    const vkBob = await VersionedKV.open(store)
    const stagedBob = new Staged(vkBob)
    const docsBob = new Namespaced(stagedBob, 'docs')

    expect(vkBob.currentCommit).toBe(initCommit)
    expect(await docsBob.get<Doc>('intro')).toEqual({ text: 'Welcome.', edits: 1 })

    // ---------- Concurrent edits to the same doc ----------
    // Alice edits the intro.
    const aliceIntro = (await docs.get<Doc>('intro')) as Doc
    docs.set('intro', { text: `${aliceIntro.text} - alice was here.`, edits: 2 })
    await stagedAlice.commit({ info: { author: 'alice', kind: 'edit' } })

    // Bob, still based on initCommit, edits the same doc.
    const bobIntro = (await docsBob.get<Doc>('intro')) as Doc
    docsBob.set('intro', { text: `${bobIntro.text} - bob too.`, edits: 2 })

    // Without a merge fn, Bob's commit would throw MergeConflict.
    // Register a domain-aware merge: concatenate distinct text
    // additions, sum the edit counts above the base.
    const docMerge: MergeFn<Doc> = (oldDoc, ours, theirs) => {
      const base = oldDoc ?? { text: '', edits: 0 }
      const o = ours ?? base
      const t = theirs ?? base
      // Concatenate the unique tail of each side relative to base.text.
      const oTail = o.text.startsWith(base.text) ? o.text.slice(base.text.length) : o.text
      const tTail = t.text.startsWith(base.text) ? t.text.slice(base.text.length) : t.text
      return {
        text: `${base.text}${tTail}${oTail}`,
        edits: base.edits + (o.edits - base.edits) + (t.edits - base.edits),
      }
    }
    stagedBob.setMergeFn('docs/intro', docMerge)

    const r2 = await stagedBob.commit({ info: { author: 'bob', kind: 'merge' } })
    expect(r2.merged).toBe(true)
    expect(r2.strategy).toBe('three_way')
    expect(r2.autoMergedKeys).toEqual(['docs/intro'])

    const merged = (await docsBob.get<Doc>('intro')) as Doc
    expect(merged.text).toBe('Welcome. - alice was here. - bob too.')
    // Alice +1, Bob +1, base 1 → merged edits = 3.
    expect(merged.edits).toBe(3)

    // ---------- Branching ----------
    const draft = (await vkBob.createBranch('draft')) as VersionedKV
    const stagedDraft = new Staged(draft)
    const docsDraft = new Namespaced(stagedDraft, 'docs')
    docsDraft.set('intro', { text: 'Draft completely different.', edits: 99 })
    await stagedDraft.commit({ info: { author: 'bob', kind: 'draft' } })

    // The draft's writes don't bleed back to main.
    const mainPeek = (await vkBob.peek('docs/intro', { branch: 'main' })) as Uint8Array
    const peeked = JSON.parse(new TextDecoder().decode(mainPeek)) as Doc
    expect(peeked.text).toBe('Welcome. - alice was here. - bob too.')

    // ---------- History walk ----------
    const linearHistory: string[] = []
    for await (const c of vkBob.history()) linearHistory.push(c)
    // Should be [merge, alice-edit, init, root-empty]
    expect(linearHistory.length).toBe(4)
    expect(linearHistory[0]).toBe(vkBob.currentCommit)

    // allParents picks up the merge commit's second parent too.
    const fullHistory = new Set<string>()
    for await (const c of vkBob.history(undefined, { allParents: true })) {
      fullHistory.add(c)
    }
    expect(fullHistory.size).toBeGreaterThanOrEqual(linearHistory.length)

    // ---------- Diff between two commits ----------
    const d = await vkBob.diff(initCommit, vkBob.currentCommit)
    // Only 'docs/intro' should show as modified (alice's profile keys were
    // already there at initCommit).
    expect([...d.modified]).toEqual(['docs/intro'])
    expect([...d.added]).toEqual([])
    expect([...d.removed]).toEqual([])

    // ---------- Checkout a historical view ----------
    const oldView = (await vkBob.checkout(initCommit)) as VersionedKV
    const oldStaged = new Staged(oldView)
    const oldDocs = new Namespaced(oldStaged, 'docs')
    expect(await oldDocs.get<Doc>('intro')).toEqual({ text: 'Welcome.', edits: 1 })
    // Live handle is unaffected.
    expect(((await docsBob.get<Doc>('intro')) as Doc).text).toBe(
      'Welcome. - alice was here. - bob too.',
    )

    // ---------- listBranches ----------
    expect(await vkBob.listBranches()).toEqual(['draft', 'main'])

    // ---------- Delete the draft branch + GC ----------
    await vkBob.deleteBranch('draft')
    expect(await vkBob.listBranches()).toEqual(['main'])

    // The draft commit is now unreachable; cleanOrphans (with minAge=-1
    // for test purposes) sweeps it along with its blobs.
    const removed = await vkBob.cleanOrphans({ minAge: -1 })
    expect(removed).toBeGreaterThanOrEqual(1)

    // Live state survives the sweep.
    expect(((await docsBob.get<Doc>('intro')) as Doc).text).toBe(
      'Welcome. - alice was here. - bob too.',
    )
    expect(await aliceProfile.get<string>('name')).toBe('Alice')
  })
})
