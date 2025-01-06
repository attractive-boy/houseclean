/**
 * Notes: 用户模块控制器
 * Ver : CCMiniCloud Framework 2.0.1 ALL RIGHTS RESERVED BY cclinux@qq.com
 * Date: 2020-09-29 04:00:00 
 */

const BaseController = require('./base_controller.js');

class UserController extends BaseController {



	/** 获取openid */
	async getOpenid() {
		// 取得数据
		let input = this.validateData(rules);
        console.log('input', input);

	}



}

module.exports = UserController;