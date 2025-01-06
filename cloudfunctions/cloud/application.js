/**
 * Notes: 云函数业务主逻辑
 * Ver : CCMiniCloud Framework 2.0.1 ALL RIGHTS RESERVED BY cclinux0730 (wechat)
 * Date: 2020-09-05 04:00:00 
 */
const Koa = require('koa');
const Router = require('koa-router');
const util = require('./framework/utils/util.js');

const timeUtil = require('./framework/utils/time_util.js');
const appUtil = require('./framework/core/app_util.js');
const appCode = require('./framework/core/app_code.js');
const appOther = require('./framework/core/app_other.js');
const config = require('./config/config.js');
const routes = require('./config/route.js');
const bodyParser = require('koa-bodyparser');
const path = require('path');

const app = new Koa();
app.use(bodyParser());
const router = new Router();

router.post('/api', async (ctx) => {

	let event = ctx.request.body;
	console.log('event', event);

	// 取得openid
	const wxContext = { OPENID: event.openid || '' };
	let r = '';
	if (!event.openid) {
		const url = 'https://api.weixin.qq.com/sns/jscode2session?appid=' + config.APPID + '&secret=' + config.APPSECRET + '&js_code=' + event.params.code + '&grant_type=authorization_code';
		const response = await fetch(url);
		const data = await response.json();
		ctx.body = appUtil.handlerData(data, r);
		return;
	}

	try {
		if (!util.isDefined(event.route)) {
			showEvent(event);
			console.error('Route Not Defined');
			ctx.body = appUtil.handlerSvrErr();
			return;
		}

		r = event.route.toLowerCase();
		if (!r.includes('/')) {
			showEvent(event);
			console.error('Route Format error[' + r + ']');
			ctx.body = appUtil.handlerSvrErr();
			return;
		}

		// 路由不存在
		if (!util.isDefined(routes[r])) {
			showEvent(event);
			console.error('Route [' + r + '] Is Not Exist');
			ctx.body = appUtil.handlerSvrErr();
			return;
		}

		let routesArr = routes[r].split('@');
		let controllerName = routesArr[0].toLowerCase().trim();
		let actionName = routesArr[1];

		// 事前处理
		if (actionName.includes('#')) {
			let actionNameArr = actionName.split('#');
			actionName = actionNameArr[0];
			if (actionNameArr[1] && config.IS_DEMO) {
				console.log('###演示版事前处理, APP Before = ' + actionNameArr[1]);
				ctx.body = beforeApp(actionNameArr[1]);
				return;
			}
		}

		// 引入逻辑controller 
		const ControllerClass = require(path.join(__dirname, 'project/controller', controllerName + '.js'));
		const controller = new ControllerClass(r, wxContext.OPENID, event);

		// 调用方法    
		await controller['initSetup']();
		let result = await controller[actionName]();


		ctx.body = result ? appUtil.handlerData(result, r) : appUtil.handlerSucc(r);
		

	} catch (ex) {
		console.error(`Exception MSG = ${ex}`);
		ctx.body = appUtil.handlerSvrErr();
		throw ex;
	}
});

// 事前处理
function beforeApp(method) {
	switch (method) {
		case 'noDemo': {
			return appUtil.handlerAppErr('本系统仅为客户体验演示，后台提交的操作均不生效！如有需要请联系作者微信cclinux0730', appCode.LOGIC);
		}
	}
	console.error('事前处理, Method Not Find = ' + method);
}

// 展示当前输入数据
function showEvent(event) {
	console.log(event);
}

app.use(router.routes()).use(router.allowedMethods());
app.listen(3000, () => {
	console.log('Koa server is running on http://localhost:3000');
});