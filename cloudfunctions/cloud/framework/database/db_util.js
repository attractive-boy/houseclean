/**
 * Notes: 数据库基本操作
 * Ver : CCMiniCloud Framework 2.0.1 ALL RIGHTS RESERVED BY cclinux@qq.com
 * Date: 2020-09-05 04:00:00 
 */
const { MongoClient } = require('mongodb');
const util = require('../utils/util.js');
const dataUtil = require('../utils/data_util.js');

// 从配置文件获取数据库配置

const { client } = require('./mongo_client.js');

const DB_NAME = 'houseclean';
const db = client.db(DB_NAME);

const MAX_RECORD_SIZE = 1000; 
const DEFAULT_RECORD_SIZE = 20;

/**
 * 批量添加数据
 * @param {*} collectionName 
 * @param {*} data 
 * @returns 返回PK
 */
async function insertBatch(collectionName, data, size = 1000) {
	let dataArr = dataUtil.spArr(data, size);
	for (let k in dataArr) {
		await db.collection(collectionName).insertMany(dataArr[k]);
	}
}

/**
 * 添加数据
 * @param {*} collectionName 
 * @param {*} data 
 * @returns 返回PK
 */
async function insert(collectionName, data) {
	const result = await db.collection(collectionName).insertOne(data);
	return result.insertedId;
}

/**
 * 更新数据
 * @param {*} collectionName 
 * @param {*} where 为非对象 则作为PK处理
 * @param {*} data 
 * @returns 影响行数
 */
async function edit(collectionName, where, data) {
	let query;
	
	// 查询条件
	if (util.isDefined(where)) {
		if (typeof (where) == 'string' || typeof (where) == 'number') {
			query = { _id: where };
		} else {
			query = fmtWhere(where);
		}
	}

	const result = await db.collection(collectionName).updateMany(query, { $set: data });
	return result.modifiedCount;
}

/**
 * 字段自增
 * @param {*} collectionName 
 * @param {*} where 为非对象 则作为PK处理
 * @param {*} field 
 * @param {*} val 
 * @returns 影响行数
 */
async function inc(collectionName, where, field, val = 1) {
	let query;
	
	// 查询条件
	if (util.isDefined(where)) {
		if (typeof (where) == 'string' || typeof (where) == 'number') {
			query = { _id: where };
		} else {
			query = fmtWhere(where);
		}
	}

	const result = await db.collection(collectionName).updateMany(query, {
		$inc: { [field]: val }
	});

	return result.modifiedCount;
}

/**
 * 自乘量，可正可负
 * @param {*} collectionName 
 * @param {*} where 为非对象 则作为PK处理
 * @param {*} field 
 * @param {*} val 
 * @returns 影响行数
 */
async function mul(collectionName, where, field, val = 1) {
	let query;
	
	// 查询条件
	if (util.isDefined(where)) {
		if (typeof (where) == 'string' || typeof (where) == 'number') {
			query = { _id: where };
		} else {
			query = fmtWhere(where);
		}
	}

	const result = await db.collection(collectionName).updateMany(query, {
		$mul: { [field]: val }
	});

	return result.modifiedCount;
}

/**
 * 删除数据
 * @param {*} collectionName 
 * @param {*} where 
 * @returns 删除函数
 */
async function del(collectionName, where) {
	let query;
	
	// 查询条件
	if (util.isDefined(where)) {
		if (typeof (where) == 'string' || typeof (where) == 'number') {
			query = { _id: where };
		} else {
			query = fmtWhere(where);
		}
	}

	const result = await db.collection(collectionName).deleteMany(query);
	return result.deletedCount;
}

/**
 * 获取总数
 * @param {*} collectionName 
 * @param {*} where 
 */
async function count(collectionName, where) {
	let query;
	
	// 查询条件
	if (typeof (where) == 'string' || typeof (where) == 'number') {
		query = { _id: where };
	} else {
		query = fmtWhere(where);
	}

	return await db.collection(collectionName).countDocuments(query);
}


/**
 * 求不重复
 * @param {*} collectionName 
 * @param {*} field 求和字段
 * @param {*} where 
 */
async function distinct(collectionName, where, field) {
	let query = fmtWhere(where);

	const result = await db.collection(collectionName).aggregate([
		{ $match: query },
		{
			$group: {
				_id: null,
				uniqueValues: { $addToSet: '$' + field }
			}
		}
	]).toArray();

	if (result.length > 0 && result[0].uniqueValues) {
		return result[0].uniqueValues;
	}
	return [];
}

async function distinctCnt(collectionName, where, field) {
	let data = await distinct(collectionName, where, field);
	return data.length;
}

/**
 * 分组求和
 * @param {*} collectionName 
 * @param {*} groupField 分组字段
 * @param {*} field 求和字段 [field1,field2,field3....]
 * @param {*} where 
 */
async function groupSum(collectionName, where, groupField, fields) {
	if (!Array.isArray(fields)) {
		fields = [fields];
	}

	let group = {
		_id: '$' + groupField
	};

	// 构建求和字段
	fields.forEach(field => {
		group[field] = { $sum: '$' + field };
	});

	const result = await db.collection(collectionName).aggregate([
		{ $match: fmtWhere(where) },
		{ $group: group }
	]).toArray();

	// 格式化返回结果
	return result.map(item => {
		let doc = { [groupField]: item._id };
		fields.forEach(field => {
			doc[field] = item[field];
		});
		return doc;
	});
}


/**
 * 分组求COUNT
 * @param {*} collectionName 
 * @param {*} groupField 分组字段 
 * @param {*} where 
 */
async function groupCount(collectionName, where, groupField) {
	const result = await db.collection(collectionName).aggregate([
		{ $match: fmtWhere(where) },
		{
			$group: {
				_id: '$' + groupField,
				total: { $sum: 1 }
			}
		}
	]).toArray();

	// 格式化返回结果
	let ret = {};
	result.forEach(item => {
		ret[groupField + '_' + item._id] = item.total;
	});
	return ret;
}

/**
 * 求和
 * @param {*} collectionName 
 * @param {*} field 求和字段
 * @param {*} where 
 */
async function sum(collectionName, where, field) {
	const result = await db.collection(collectionName).aggregate([
		{ $match: fmtWhere(where) },
		{
			$group: {
				_id: null,
				summ: { $sum: '$' + field }
			}
		}
	]).toArray();

	if (result.length > 0 && result[0].summ) {
		return result[0].summ;
	}
	return 0;
}

/**
 * 最大
 */
async function max(collectionName, where, field) {
	const result = await db.collection(collectionName).aggregate([
		{ $match: fmtWhere(where) },
		{
			$group: {
				_id: null,
				maxx: { $max: '$' + field }
			}
		}
	]).toArray();

	if (result.length > 0 && result[0].maxx) {
		return result[0].maxx;
	}
	return 0;
}

/**
 * 最小
 */
async function min(collectionName, where, field) {
	const result = await db.collection(collectionName).aggregate([
		{ $match: fmtWhere(where) },
		{
			$group: {
				_id: null,
				minx: { $min: '$' + field }
			}
		}
	]).toArray();

	if (result.length > 0 && result[0].minx) {
		return result[0].minx;
	}
	return 0;
}

/**
 * 清空数据
 */
async function clear(collectionName) {
	await db.collection(collectionName).deleteMany({});
}

/**
 * 检查集合是否存在
 */
async function isExistCollection(collectionName) {
	try {
		const collections = await db.listCollections({ name: collectionName }).toArray();
		return collections.length > 0; // 如果集合存在，返回 true
	} catch (err) {
		console.log('### isExistCollection...', err);
		return false;
	}
}

/**
 * 创建集合
 */
async function createCollection(collectionName) {
	try {
		await db.createCollection(collectionName);
		console.log('>> Create New Collection [' + collectionName + '] Succ, OVER.');
		return true;
	} catch (err) {
		console.error('>> Create New Collection [' + collectionName + '] Failed, Code=' + err.code + '|' + err.message);
		return false;
	}
}

/**
 * 随机获取数据
 */
async function rand(collectionName, where = {}, fields = '*', size = 1) {
	size = Number(size);
	if (size > MAX_RECORD_SIZE) size = MAX_RECORD_SIZE;

	let pipeline = [];

	// 查询条件
	if (util.isDefined(where)) {
		pipeline.push({ $match: fmtWhere(where) });
	}

	// 字段筛选
	if (util.isDefined(fields) && fields != '*') {
		pipeline.push({ $project: fmtFields(fields) });
	}

	// 随机抽取
	pipeline.push({ $sample: { size } });

	const result = await db.collection(collectionName).aggregate(pipeline).toArray();

	if (size == 1) {
		return result.length ? result[0] : null;
	}
	return result;
}

/**
 * 数组字段拆分查询
 */
async function getListByArray(collectionName, arrField, where, fields, orderBy, page = 1, size = DEFAULT_RECORD_SIZE, isTotal = true, oldTotal = 0) {
	if (typeof (where) == 'string' || typeof (where) == 'number') {
		where = { _id: where };
	}

	page = Number(page);
	size = Number(size);
	if (size > MAX_RECORD_SIZE) size = MAX_RECORD_SIZE;

	let data = {
		page,
		size
	};

	let offset = 0;

	// 计算总数
	if (isTotal) {
		const total = await count(collectionName, where);
		data.total = total;
		data.count = Math.ceil(total / size);

		if (page > 1 && oldTotal > 0) {
			offset = data.total - oldTotal;
			if (offset < 0) offset = 0;
		}
	}

	let pipeline = [
		{ $unwind: '$' + arrField }
	];

	// 查询条件
	if (util.isDefined(where)) {
		pipeline.push({ $match: fmtWhere(where) });
	}

	// 字段筛选
	if (util.isDefined(fields) && fields != '*') {
		pipeline.push({ $project: fmtFields(fields) });
	}

	// 排序
	if (util.isDefined(orderBy)) {
		pipeline.push({ $sort: fmtJoinSort(orderBy) });
	}

	// 分页
	pipeline.push({ $skip: (page - 1) * size + offset });
	pipeline.push({ $limit: size });

	data.list = await db.collection(collectionName).aggregate(pipeline).toArray();

	return data;
}

/**
 * 联表查询获取分页数据
 */
async function getListJoin(collectionName, joinParams, where, fields, orderBy, page = 1, size = DEFAULT_RECORD_SIZE, isTotal = true, oldTotal = 0, is2Many = false) {
	if (typeof (where) == 'string' || typeof (where) == 'number') {
		where = { _id: where };
	}

	page = Number(page);
	size = Number(size);
	if (size > MAX_RECORD_SIZE) size = MAX_RECORD_SIZE;

	let data = {
		page,
		size
	};

	let offset = 0;

	// 计算总数
	if (isTotal) {
		let pipeline = [
			{ $lookup: joinParams },
			{ $match: fmtWhere(where) }
		];
		
		const countResult = await db.collection(collectionName).aggregate([
			...pipeline,
			{ $count: 'total' }
		]).toArray();

		const total = countResult.length ? countResult[0].total : 0;
		data.total = total;
		data.count = Math.ceil(total / size);

		if (page > 1 && oldTotal > 0) {
			offset = data.total - oldTotal;
			if (offset < 0) offset = 0;
		}
	}

	let pipeline = [
		{ $lookup: joinParams }
	];

	// 查询条件
	if (util.isDefined(where)) {
		pipeline.push({ $match: fmtWhere(where) });
	}

	// 字段筛选
	if (util.isDefined(fields) && fields != '*') {
		pipeline.push({ $project: fmtFields(fields) });
	}

	// 排序
	if (util.isDefined(orderBy)) {
		pipeline.push({ $sort: fmtJoinSort(orderBy) });
	}

	// 分页
	pipeline.push({ $skip: (page - 1) * size + offset });
	pipeline.push({ $limit: size });

	data.list = await db.collection(collectionName).aggregate(pipeline).toArray();

	// 1:N 数据处理为1:1
	if (!is2Many) {
		data.list.forEach(item => {
			if (util.isDefined(item[joinParams.as])) {
				if (Array.isArray(item[joinParams.as]) && item[joinParams.as].length > 0) {
					item[joinParams.as] = item[joinParams.as][0];
				} else {
					item[joinParams.as] = {};
				}
			}
		});
	}

	return data;
}

/**
 * 获取分页数据
 */
async function getList(collectionName, where, fields = '*', orderBy = {}, page = 1, size = DEFAULT_RECORD_SIZE, isTotal = true, oldTotal = 0) {
	page = Number(page);
	size = Number(size);

	if (size > MAX_RECORD_SIZE || size == 0) size = MAX_RECORD_SIZE;

	let data = {
		page,
		size
	};

	let offset = 0;

	// 计算总数
	if (isTotal) {
		const total = await count(collectionName, where);
		data.total = total;
		data.count = Math.ceil(total / size);

		if (page > 1 && oldTotal > 0) {
			offset = data.total - oldTotal;
			if (offset < 0) offset = 0;
		}
	}

	let query = db.collection(collectionName).find(fmtWhere(where));

	// 字段筛选
	if (fields != '*') {
		query = query.project(fmtFields(fields));
	}

	// 排序
	if (Object.keys(orderBy).length > 0) {
		query = query.sort(fmtJoinSort(orderBy));
	}

	// 分页
	query = query.skip((page - 1) * size + offset).limit(size);

	data.list = await query.toArray();
	return data;
}

/**
 * 大数据情况下取得所有数据
 */
async function getAllBig(collectionName, where, fields = '*', orderBy, size = MAX_RECORD_SIZE) {
	size = Number(size);
	if (size > MAX_RECORD_SIZE) size = MAX_RECORD_SIZE;

	// 计算总数
	const total = await count(collectionName, where);

	// 页数
	const page = Math.ceil(total / size);

	let list = [];
	for (let i = 1; i <= page; i++) {
		let data = await getList(collectionName, where, fields, orderBy, i, size, false);
		if (data && data.list) {
			list = list.concat(data.list);
		}
	}

	return list;
}

/**
 * 获取所有数据
 */
async function getAll(collectionName, where, fields = '*', orderBy, size = MAX_RECORD_SIZE) {
	size = Number(size);
	if (size > MAX_RECORD_SIZE) size = MAX_RECORD_SIZE;

	let query = db.collection(collectionName).find(fmtWhere(where));

	// 字段筛选
	if (fields != '*') {
		query = query.project(fmtFields(fields));
	}

	// 排序
	if (orderBy) {
		query = query.sort(fmtJoinSort(orderBy));
	}

	query = query.limit(size);
	return await query.toArray();
}

/**
 * 获取所有数据 数组字段拆分查询
 */
async function getAllByArray(collectionName, arrField, where, fields = '*', orderBy, size = MAX_RECORD_SIZE) {
	size = Number(size);
	if (size > MAX_RECORD_SIZE) size = MAX_RECORD_SIZE;

	// 拆分
	let query = await db.collection(collectionName).aggregate()
		.unwind('$' + arrField);

	// 查询条件
	if (util.isDefined(where))
		query = await query.match(fmtWhere(where));

	// 取出特定字段
	if (util.isDefined(fields) && fields != '*')
		query = await query.project(fmtFields(fields));

	// 排序 
	if (util.isDefined(orderBy)) {
		query = await query.sort(fmtJoinSort(orderBy));
	}

	// 取数据
	query = await query.limit(size).end();
	return query.list;
}

/**
 * 获取单个数据
 */
async function getOne(collectionName, where, fields = '*', orderBy = {}) {
	// 根据ID查询还是根据条件查询
	if (typeof (where) == 'string' || typeof (where) == 'number') {
		where = { _id: where };
	}

	let query = db.collection(collectionName).find(fmtWhere(where));

	// 字段筛选
	if (fields != '*') {
		query = query.project(fmtFields(fields));
	}

	// 排序
	if (Object.keys(orderBy).length > 0) {
		query = query.sort(fmtJoinSort(orderBy));
	}

	query = query.limit(1);
	const result = await query.toArray();
	return result.length > 0 ? result[0] : null;
}

/**
 * 格式化排序条件
 */
function fmtJoinSort(sort) {
	let result = {};
	for (let k in sort) {
		let v = sort[k];
		if (typeof (v) == 'string') {
			v = v.toLowerCase();
			result[k] = v === 'asc' ? 1 : -1;
		} else {
			result[k] = v;
		}
	}
	return result;
}

/**
 * 格式化字段条件
 */
function fmtFields(fields) {
	if (typeof (fields) == 'string') {
		let obj = {};
		fields = fields.replace(/，/g, ',');
		fields.split(',').forEach(field => {
			if (field.trim().length > 0) {
				obj[field.trim()] = 1;
			}
		});
		return obj;
	}
	return fields;
}

/**
 * 格式化查询条件
 */
function fmtWhere(where) {
	if (!where) return {};

	if (util.isDefined(where.and) || util.isDefined(where.or)) {
		let whereEx = {};
		if (util.isDefined(where.and)) {
			whereEx.$and = [fmtWhere(where.and)];
		}

		if (util.isDefined(where.or)) {
			whereEx.$or = Array.isArray(where.or) ? 
				where.or.map(w => fmtWhere(w)) : 
				[fmtWhere(where.or)];
		}
		return whereEx;
	}

	// 如果是数组 一般是用在or的或条件
	if (Array.isArray(where)) {
		return where.map(w => fmtWhere(w));
	}

	let result = {};
	for (let k in where) {
		if (Array.isArray(where[k])) {
			if (!Array.isArray(where[k][0]) && where[k][0].toLowerCase().trim() == 'between') {
				// between条件特殊处理
				result[k] = {
					$gte: where[k][1],
					$lte: where[k][2]
				};
			} else {
				// 处理多条件
				let conditions = Array.isArray(where[k][0]) ? where[k] : [where[k]];
				result[k] = {};
				conditions.forEach(condition => {
					Object.assign(result[k], fmtWhereSimple(condition));
				});
			}
		} else {
			result[k] = where[k];
		}
	}
	return result;
}

/**
 * 格式化单个查询条件
 */
function fmtWhereSimple(arr) {
	let op = arr[0].toLowerCase().trim();
	let val = arr[1];
	
	switch (op) {
		case '=': return { $eq: val };
		case '!=':
		case '<>': return { $ne: val };
		case '<': return { $lt: val };
		case '<=': return { $lte: val };
		case '>': return { $gt: val };
		case '>=': return { $gte: val };
		case 'like': 
			if (!util.isDefined(val) || !val) return {};
			return { $regex: val, $options: 'i' };
		case 'in':
			return { $in: dataUtil.str2Arr(val) };
		case 'not in':
			return { $nin: dataUtil.str2Arr(val) };
		default:
			console.error('error where oprt=' + op);
			return {};
	}
}

module.exports = {
	insert,
	insertBatch,
	edit,
	del,

	count,
	inc,
	sum,
	groupCount,
	groupSum,
	distinct,
	distinctCnt,
	max,
	min,
	mul, // 原子操作，用于指示字段自乘某个值

	isExistCollection,
	createCollection,
	clear,
	rand,
	getOne,
	getAll,
	getAllBig,

	getAllByArray,
	getList,
	getListJoin,
	getListByArray,
	MAX_RECORD_SIZE,
	DEFAULT_RECORD_SIZE
}