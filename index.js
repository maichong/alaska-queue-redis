/**
 * @copyright Maichong Software Ltd. 2016 http://maichong.it
 * @date 2016-03-28
 * @author Liang <liang@maichong.it>
 */

'use strict';

const redis = require('redis');

class RedisQueueDriver {

  constructor(options) {
    this.key = options.key;
    this.options = options;
    this.isQueueDriver = true;
    this._driver = redis.createClient(options);
  }

  /**
   * [async] 将元素插入队列
   * @param {*} item
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
   * @param {number} timeout 超时时间,单位毫秒,默认不阻塞,为0则永久阻塞
   * @returns {*}
   */
  pop(timeout) {
    let method = timeout === undefined ? 'lpop' : 'blpop';
    return new Promise((resolve, reject)=> {
      let args = [this.key];
      if (method === 'blpop') {
        args.push(parseInt(timeout / 1000));
      }
      args.push(function (error, res) {
        if (error) {
          reject(error);
        } else {
          if (res !== null) {
            try {
              if (Array.isArray(res)) {
                res = res[1];
              }
              res = JSON.parse(res);
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
   * 释放当前所有任务,进入空闲状态
   */
  free() {
  }

  /**
   * 销毁队列
   */
  destroy() {
    this._driver.quit();
    this._driver = null;
  }
}

module.exports = RedisQueueDriver.default = RedisQueueDriver;
