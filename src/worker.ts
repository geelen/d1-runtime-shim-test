import { ExportedHandler } from '@cloudflare/workers-types/2022-11-30/index'
import { expect, z } from 'z-expect'

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
					test_d1_select_all,
					test_d1_select_one,
					test_d1_batch,
					test_d1_exec,
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
	expect(await stmt.all()).toMatchObject({
		results: [user_1, user_2],
		meta: { duration: z.number().gte(0), served_by: 'v3-prod' },
		success: true,
	})

	const [raw, first, firstColumn] = await Promise.all([stmt.raw(), stmt.first(), stmt.first('features')])
	expect(raw).toMatchObject([Object.values(user_1), Object.values(user_2)])
	expect(first).toMatchObject(user_1)
	expect(firstColumn).toEqual(user_1.features)
}

async function test_d1_select_one(DB: D1Database) {
	const user_1 = MOCK_USER_ROWS[1]
	const user_2 = MOCK_USER_ROWS[2]

	const withParam = DB.prepare(`select *from users where user_id = ?;`)
	{
		const stmt = withParam.bind(1)
		expect(await stmt.all()).toMatchObject({
			results: [user_1],
			meta: { duration: z.number().gte(0), served_by: 'v3-prod' },
			success: true,
		})

		const [raw, first, firstColumn] = await Promise.all([stmt.raw(), stmt.first(), stmt.first('home')])
		expect(raw).toMatchObject([Object.values(user_1)])
		expect(first).toMatchObject(user_1)
		expect(firstColumn).toEqual(user_1.home)
	}
	{
		const stmt = withParam.bind(2)
		expect(await stmt.all()).toMatchObject({
			results: [user_2],
			meta: { duration: z.number().gte(0), served_by: 'v3-prod' },
			success: true,
		})

		const [raw, first, firstColumn] = await Promise.all([stmt.raw(), stmt.first(), stmt.first('name')])
		expect(raw).toMatchObject([Object.values(user_2)])
		expect(first).toMatchObject(user_2)
		expect(firstColumn).toEqual(user_2.name)
	}
}

async function test_d1_batch(DB: D1Database) {
	const user_1 = MOCK_USER_ROWS[1]
	const user_2 = MOCK_USER_ROWS[2]

	const withParam = DB.prepare(`select *from users where user_id = ?;`)
	const response = await DB.batch([withParam.bind(1), withParam.bind(2)])
	expect(response).toMatchObject([
		{
			results: [user_1],
			meta: { duration: z.number().gte(0), served_by: 'v3-prod' },
			success: true,
		},
		{
			results: [user_2],
			meta: { duration: z.number().gte(0), served_by: 'v3-prod' },
			success: true,
		},
	])
}

async function test_d1_exec(DB: D1Database) {
	const response = await DB.exec(`
			select 1;
			select * from users;
		`)
	expect(response).toMatchObject({ count: 2, duration: z.number().gte(0) })
}
