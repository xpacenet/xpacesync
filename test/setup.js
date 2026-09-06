// MessageLog/ContentCache use the real IndexedDB API — provide it in the
// Node test environment via fake-indexeddb so tests exercise the same code
// path a browser would run, not a mocked-out substitute.
import 'fake-indexeddb/auto'
