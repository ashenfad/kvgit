/**
 * End-to-end smoke test.
 *
 * One realistic scenario that exercises the full stack:
 *
 *   Repo (JSON codec) → Worktree → Memory
 *          │              └─ Namespaced views
 *          └─ branches, snapshots, log, diff, gc
 *
 * Doubles as a runnable example: if you read one test to understand
 * what the API looks like in practice, read this one.
 */

import { describe, expect, it } from 'vitest'
import { Memory } from '../src/backends/memory'
import { type MergeFn, Namespaced, Repo } from '../src/index'

interface Doc {
  text: string
  edits: number
}

describe('e2e: collaborative document store', () => {
  it('two writers concurrently edit a shared doc through Namespaced views', async () => {
    // ---------- Setup ----------
    const store = new Memory()

    // Open the repository with the JSON codec and check out `main`.
    // Writes stay pending in the worktree until we commit.
    const repoAlice = await Repo.open(store, { codec: 'json' })
    const wtAlice = await repoAlice.worktree('main', { create: true })

    // Two namespaced views over the same worktree — Alice writes only
    // under `alice/*`, the shared doc lives under `docs/*`.
    const aliceProfile = new Namespaced(wtAlice, 'alice')
    const docs = new Namespaced(wtAlice, 'docs')

    // ---------- Alice's first commit ----------
    aliceProfile.set('name', 'Alice')
    aliceProfile.set('email', 'alice@example.com')
    docs.set('intro', { text: 'Welcome.', edits: 1 } satisfies Doc)

    expect(wtAlice.status().clean).toBe(false)

    const r1 = await wtAlice.commit({ info: { author: 'alice', kind: 'init' } })
    expect(r1.merged).toBe(true)
    expect(r1.strategy).toBe('fast_forward')
    expect(wtAlice.status().clean).toBe(true)

    const initCommit = wtAlice.head
    expect((await repoAlice.getCommit(initCommit)).info).toEqual({
      author: 'alice',
      kind: 'init',
    })

    // ---------- Read-back through namespaced views ----------
    expect(await aliceProfile.get('name')).toBe('Alice')
    const intro = (await docs.get('intro')) as Doc | undefined
    expect(intro?.text).toBe('Welcome.')
    expect(intro?.edits).toBe(1)

    // keys() lists the namespace's direct children.
    expect(await docs.keys()).toEqual(['intro'])

    // ---------- Bob opens a second repository on the same store ----------
    const repoBob = await Repo.open(store, { codec: 'json' })
    const wtBob = await repoBob.worktree('main')
    const docsBob = new Namespaced(wtBob, 'docs')

    expect(wtBob.head).toBe(initCommit)
    expect(await docsBob.get('intro')).toEqual({ text: 'Welcome.', edits: 1 })

    // ---------- Concurrent edits to the same doc ----------
    // Alice edits the intro.
    const aliceIntro = (await docs.get('intro')) as Doc
    docs.set('intro', { text: `${aliceIntro.text} - alice was here.`, edits: 2 })
    await wtAlice.commit({ info: { author: 'alice', kind: 'edit' } })

    // Bob, still based on initCommit, edits the same doc.
    const bobIntro = (await docsBob.get('intro')) as Doc
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
    wtBob.setMergeFn('docs/intro', docMerge as MergeFn<unknown>)

    const r2 = await wtBob.commit({ info: { author: 'bob', kind: 'merge' } })
    expect(r2.merged).toBe(true)
    expect(r2.strategy).toBe('three_way')
    expect(r2.autoMergedKeys).toEqual(['docs/intro'])

    const merged = (await docsBob.get('intro')) as Doc
    expect(merged.text).toBe('Welcome. - alice was here. - bob too.')
    // Alice +1, Bob +1, base 1 → merged edits = 3.
    expect(merged.edits).toBe(3)

    // ---------- Branching ----------
    await repoBob.branches.create('draft', wtBob.head)
    const wtDraft = await repoBob.worktree('draft')
    const docsDraft = new Namespaced(wtDraft, 'docs')
    docsDraft.set('intro', { text: 'Draft completely different.', edits: 99 })
    await wtDraft.commit({ info: { author: 'bob', kind: 'draft' } })

    // The draft's writes don't bleed back to main.
    const mainRaw = (await (
      await repoBob.snapshot({ branch: 'main' })
    ).raw.get('docs/intro')) as Uint8Array
    const peeked = JSON.parse(new TextDecoder().decode(mainRaw)) as Doc
    expect(peeked.text).toBe('Welcome. - alice was here. - bob too.')

    // ---------- History walk ----------
    const linearHistory: string[] = []
    for await (const c of repoBob.log({ branch: 'main' }, { firstParent: true })) {
      linearHistory.push(c.hash)
    }
    // Should be [merge, alice-edit, init, root-empty]
    expect(linearHistory.length).toBe(4)
    expect(linearHistory[0]).toBe(wtBob.head)

    // The full log picks up the merge commit's second parent too.
    const fullHistory = new Set<string>()
    for await (const c of repoBob.log({ branch: 'main' })) fullHistory.add(c.hash)
    expect(fullHistory.size).toBeGreaterThanOrEqual(linearHistory.length)

    // ---------- Diff between two commits ----------
    const d = await repoBob.diff(initCommit, wtBob.head)
    // Only 'docs/intro' should show as modified (alice's profile keys were
    // already there at initCommit).
    expect([...d.modified]).toEqual(['docs/intro'])
    expect([...d.added]).toEqual([])
    expect([...d.removed]).toEqual([])

    // ---------- A snapshot of a historical commit ----------
    const oldView = await repoBob.snapshot({ commit: initCommit })
    expect(await oldView.get('docs/intro')).toEqual({ text: 'Welcome.', edits: 1 })
    // The live worktree is unaffected.
    expect(((await docsBob.get('intro')) as Doc).text).toBe('Welcome. - alice was here. - bob too.')

    // ---------- Listing branches ----------
    expect(await repoBob.branches.list()).toEqual(['draft', 'main'])

    // ---------- Delete the draft branch + GC ----------
    await repoBob.branches.delete('draft')
    expect(await repoBob.branches.list()).toEqual(['main'])

    // The draft commit is now unreachable; gc (with minAge=-1 for test
    // purposes) sweeps it along with its blobs.
    const removed = await repoBob.gc({ minAge: -1 })
    expect(removed).toBeGreaterThanOrEqual(1)

    // Live state survives the sweep.
    expect(((await docsBob.get('intro')) as Doc).text).toBe('Welcome. - alice was here. - bob too.')
    expect(await aliceProfile.get('name')).toBe('Alice')
  })
})
