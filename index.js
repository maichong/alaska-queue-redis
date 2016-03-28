/**
 * @copyright Maichong Software Ltd. 2016 http://maichong.it
 * @date 2016-03-28
 * @author Liang <liang@maichong.it>
 */

'use strict';

const redis = require('redis');

class RedisQueueDriver {

  constructor(key, options) {
    this.key = key;
    this._driver = redis.createClient(options);
  }

  /**
   * [async] 将元素插入队列
   * @param {*} item
   * @returns {boolean}
   */
  push(item) {
    return new Promise((resolve, reject)=> {
      this._driver.rpush(this.key, JSON.stringify(item), function (error) {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  /**
   * [async] 读取队列中的元素
   * @param {number} timeout 超时时间,单位秒
   * @returns {boolean}
   */
  pop(timeout) {
    let method = timeout ? 'blpop' : 'lpop';
    return new Promise((resolve, reject)=> {
      let args = [this.key];
      if (timeout) {
        args.push(timeout);
      }
      args.push(function (error, res) {
        if (error) {
          reject(error);
        } else {
          if (res !== null) {
            try {
              res = JSON.parse(res[1]);
            } catch (error) {
              res = null;
            }
          }
          resolve(res);
        }
      });
      this._driver[method].apply(this._driver, args);
    });
  }

  /**
   * 销毁队列
   */
  destory() {
    this._driver = null;
  }

  /**
   * 通过Key
   * @param {string} key
   * @param {object} options
   * @returns {RedisQueueDriver}
   */
  static instance(key, options) {
    return new RedisQueueDriver(key, options);
  }
}

module.exports = RedisQueueDriver.default = RedisQueueDriver;