import { ExportedHandler } from '@cloudflare/workers-types/2022-11-30/index'
import { z, expect } from 'z-expect'

export interface Env {
	DB: D1Database
}

function safeExec(env: Env) {
	return async ([name, fn]: [string, (DB: D1Database) => Promise<void>]) => {
		try {
			await fn(env.DB)
			return { [name]: 'ok' }
		} catch (e) {
			console.log(e)
			return { [name]: `error: ${(e as Error).message}` }
		}
	}
}

// // TEST RUNNER
export default {
	async fetch(request, env, ctx) {
		const result = [
			await safeExec(env)(['init', init]),
			...(await Promise.all(
				Object.entries({
					test_d1_select_1,
					// 		test_d1_select_all,
					// 		test_d1_select_one,
					// 		test_d1_batch,
					// 		test_d1_exec,
				}).map(safeExec(env)),
			)),
		]

		return Response.json(result)
	},
} satisfies ExportedHandler<Env>

// REMAINDER COPIED FROM workerd: src/edgeworker/tests/d1/d1-api-test.js
const MOCK_USER_ROWS = {
	1: { user_id: 1, name: 'Albert Ross', home: 'sky', features: 'wingspan' },
	2: { user_id: 2, name: 'Al Dente', home: 'bowl', features: 'mouthfeel' },
}

async function init(DB: D1Database) {
	expect({
		results: [{ 1: 1 }],
		meta: { duration: 0.001, served_by: 'd1-mock' },
		success: true,
	}).toMatchObject({
		results: [{ 1: 1 }],
		meta: { duration: z.number().gt(0), served_by: 'd1-mock' },
		success: true,
	})

	await DB.batch([
		DB.prepare(`DROP TABLE IF EXISTS users;`),
		DB.prepare(`CREATE TABLE users ( user_id INTEGER PRIMARY KEY, name TEXT, home TEXT, features TEXT);`),
		DB.prepare(`INSERT INTO users (name, home, features) VALUES
			 ('Albert Ross', 'sky', 'wingspan'),
			 ('Al Dente', 'bowl', 'mouthfeel')
		;`),
	])
}

async function test_d1_select_1(DB: D1Database) {
	const stmt = DB.prepare(`select 1;`)

	const [raw, first, firstColumn] = await Promise.all([stmt.raw(), stmt.first(), stmt.first('1')])
	expect(raw).toMatchObject([[1]])
	expect(first).toMatchObject({ 1: 1 })
	expect(firstColumn).toEqual(1)
}

async function test_d1_select_all(DB: D1Database) {
	const user_1 = MOCK_USER_ROWS[1]
	const user_2 = MOCK_USER_ROWS[2]

	const stmt = DB.prepare(`select *from users;`)
	assert.deepEqual(await stmt.all(), {
		results: [user_1, user_2],
		meta: { duration: 0.001, served_by: 'd1-mock' },
		success: true,
	})

	const [raw, first, firstColumn] = await Promise.all([stmt.raw(), stmt.first(), stmt.first('features')])
	assert.deepEqual(raw, [Object.values(user_1), Object.values(user_2)])
	assert.deepEqual(first, user_1)
	assert.deepEqual(firstColumn, user_1.features)
}

async function test_d1_select_one(DB: D1Database) {
	const user_1 = MOCK_USER_ROWS[1]
	const user_2 = MOCK_USER_ROWS[2]

	const withParam = DB.prepare(`select *from users where user_id = ?;`)
	{
		const stmt = withParam.bind(1)
		assert.deepEqual(await stmt.all(), {
			results: [user_1],
			meta: { duration: 0.001, served_by: 'd1-mock' },
			success: true,
		})

		const [raw, first, firstColumn] = await Promise.all([stmt.raw(), stmt.first(), stmt.first('home')])
		assert.deepEqual(raw, [Object.values(user_1)])
		assert.deepEqual(first, user_1)
		assert.deepEqual(firstColumn, user_1.home)
	}
	{
		const stmt = withParam.bind(2)
		assert.deepEqual(await stmt.all(), {
			results: [user_2],
			meta: { duration: 0.001, served_by: 'd1-mock' },
			success: true,
		})

		const [raw, first, firstColumn] = await Promise.all([stmt.raw(), stmt.first(), stmt.first('name')])
		assert.deepEqual(raw, [Object.values(user_2)])
		assert.deepEqual(first, user_2)
		assert.deepEqual(firstColumn, user_2.name)
	}
}

async function test_d1_batch(DB: D1Database) {
	const user_1 = MOCK_USER_ROWS[1]
	const user_2 = MOCK_USER_ROWS[2]

	const withParam = DB.prepare(`select *from users where user_id = ?;`)
	const response = await DB.batch([withParam.bind(1), withParam.bind(2)])
	assert.deepEqual(response, [
		{
			results: [user_1],
			meta: { duration: 0.001, served_by: 'd1-mock' },
			success: true,
		},
		{
			results: [user_2],
			meta: { duration: 0.001, served_by: 'd1-mock' },
			success: true,
		},
	])
}

async function test_d1_exec(DB: D1Database) {
	const response = await DB.exec(`
			select 1;
			select *
			from users;
		`)
	assert.deepEqual(response, { count: 2, duration: 0.002 })
}
