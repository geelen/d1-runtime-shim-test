import * as assert from 'node:assert'

function toZodObj(obj: Record<string, unknown>) {}

function toZodValue(val: unknown) {
	if (typeof val === 'string') {
	}
}

export function expect<T = unknown>(actual: unknown) {
	const schema = toZodValue(actual)
	return {
		toMatchObject(expected: T) {},
	}
}
