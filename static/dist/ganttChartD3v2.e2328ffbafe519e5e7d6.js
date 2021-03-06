(function webpackUniversalModuleDefinition(root, factory) {
	if(typeof exports === 'object' && typeof module === 'object')
		module.exports = factory();
	else if(typeof define === 'function' && define.amd)
		define([], factory);
	else if(typeof exports === 'object')
		exports["ganttChartD3v2"] = factory();
	else
		root["Airflow"] = root["Airflow"] || {}, root["Airflow"]["ganttChartD3v2"] = factory();
})(window, function() {
return /******/ (function(modules) { // webpackBootstrap
/******/ 	// The module cache
/******/ 	var installedModules = {};
/******/
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/
/******/ 		// Check if module is in cache
/******/ 		if(installedModules[moduleId]) {
/******/ 			return installedModules[moduleId].exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = installedModules[moduleId] = {
/******/ 			i: moduleId,
/******/ 			l: false,
/******/ 			exports: {}
/******/ 		};
/******/
/******/ 		// Execute the module function
/******/ 		modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/
/******/ 		// Flag the module as loaded
/******/ 		module.l = true;
/******/
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/
/******/
/******/ 	// expose the modules object (__webpack_modules__)
/******/ 	__webpack_require__.m = modules;
/******/
/******/ 	// expose the module cache
/******/ 	__webpack_require__.c = installedModules;
/******/
/******/ 	// define getter function for harmony exports
/******/ 	__webpack_require__.d = function(exports, name, getter) {
/******/ 		if(!__webpack_require__.o(exports, name)) {
/******/ 			Object.defineProperty(exports, name, { enumerable: true, get: getter });
/******/ 		}
/******/ 	};
/******/
/******/ 	// define __esModule on exports
/******/ 	__webpack_require__.r = function(exports) {
/******/ 		if(typeof Symbol !== 'undefined' && Symbol.toStringTag) {
/******/ 			Object.defineProperty(exports, Symbol.toStringTag, { value: 'Module' });
/******/ 		}
/******/ 		Object.defineProperty(exports, '__esModule', { value: true });
/******/ 	};
/******/
/******/ 	// create a fake namespace object
/******/ 	// mode & 1: value is a module id, require it
/******/ 	// mode & 2: merge all properties of value into the ns
/******/ 	// mode & 4: return value when already ns object
/******/ 	// mode & 8|1: behave like require
/******/ 	__webpack_require__.t = function(value, mode) {
/******/ 		if(mode & 1) value = __webpack_require__(value);
/******/ 		if(mode & 8) return value;
/******/ 		if((mode & 4) && typeof value === 'object' && value && value.__esModule) return value;
/******/ 		var ns = Object.create(null);
/******/ 		__webpack_require__.r(ns);
/******/ 		Object.defineProperty(ns, 'default', { enumerable: true, value: value });
/******/ 		if(mode & 2 && typeof value != 'string') for(var key in value) __webpack_require__.d(ns, key, function(key) { return value[key]; }.bind(null, key));
/******/ 		return ns;
/******/ 	};
/******/
/******/ 	// getDefaultExport function for compatibility with non-harmony modules
/******/ 	__webpack_require__.n = function(module) {
/******/ 		var getter = module && module.__esModule ?
/******/ 			function getDefault() { return module['default']; } :
/******/ 			function getModuleExports() { return module; };
/******/ 		__webpack_require__.d(getter, 'a', getter);
/******/ 		return getter;
/******/ 	};
/******/
/******/ 	// Object.prototype.hasOwnProperty.call
/******/ 	__webpack_require__.o = function(object, property) { return Object.prototype.hasOwnProperty.call(object, property); };
/******/
/******/ 	// __webpack_public_path__
/******/ 	__webpack_require__.p = "";
/******/
/******/
/******/ 	// Load entry module and return exports
/******/ 	return __webpack_require__(__webpack_require__.s = 8);
/******/ })
/************************************************************************/
/******/ ({

/***/ 8:
/***/ (function(module, exports) {

/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * @author Dimitry Kudrayvtsev
 * @version 2.1
 * @modifiedby Maxime Beauchemin
 */
// Taken from
//https://github.com/benjaminoakes/moment-strftime/blob/1886cabc4b07d13e3046ae075d357e7aad92ea93/lib/moment-strftime.js
// but I couldn't work out how to make webpack not include moment again.
// TODO: revisit our webpack config
//
// -- Begin moment-strftime
// Copyright (c) 2012 Benjamin Oakes, MIT Licensed
const replacements = {
  'a': 'ddd',
  'A': 'dddd',
  'b': 'MMM',
  'B': 'MMMM',
  'c': 'lll',
  'd': 'DD',
  '-d': 'D',
  'e': 'D',
  'F': 'YYYY-MM-DD',
  'H': 'HH',
  '-H': 'H',
  'I': 'hh',
  '-I': 'h',
  'j': 'DDDD',
  '-j': 'DDD',
  'k': 'H',
  'l': 'h',
  'm': 'MM',
  '-m': 'M',
  'M': 'mm',
  '-M': 'm',
  'p': 'A',
  'P': 'a',
  'S': 'ss',
  '-S': 's',
  'u': 'E',
  'w': 'd',
  'W': 'WW',
  'x': 'll',
  'X': 'LTS',
  'y': 'YY',
  'Y': 'YYYY',
  'z': 'ZZ',
  'Z': 'z',
  'f': 'SSS',
  '%': '%'
};

moment.fn.strftime = function (format) {
  var momentFormat, tokens; // Break up format string based on strftime tokens

  tokens = format.split(/(%\-?.)/);
  momentFormat = tokens.map(function (token) {
    // Replace strftime tokens with moment formats
    if (token[0] === '%' && replacements.hasOwnProperty(token.substr(1))) {
      return replacements[token.substr(1)];
    } // Escape non-token strings to avoid accidental formatting


    return token.length > 0 ? '[' + token + ']' : token;
  }).join('');
  return this.format(momentFormat);
}; // -- End moment-strftime


d3.gantt = function () {
  var FIT_TIME_DOMAIN_MODE = "fit";
  var FIXED_TIME_DOMAIN_MODE = "fixed";
  var tip = d3.tip().attr('class', 'tooltip d3-tip').offset([-10, 0]).html(d => tiTooltip(d, {
    includeTryNumber: true
  }));
  var margin = {
    top: 20,
    right: 40,
    bottom: 20,
    left: 150
  };
  var yAxisLeftOffset = 220;
  var selector = 'body';
  var timeDomainStart = d3.time.day.offset(new Date(), -3);
  var timeDomainEnd = d3.time.hour.offset(new Date(), +3);
  var timeDomainMode = FIT_TIME_DOMAIN_MODE; // fixed or fit

  var taskTypes = [];
  var height = document.body.clientHeight - margin.top - margin.bottom - 5;
  var width = $('.gantt').width() - margin.right - margin.left - 5;
  var tickFormat = "%H:%M";

  var keyFunction = function (d) {
    return d.start_date + d.task_id + d.end_date;
  };

  var rectTransform = function (d) {
    return "translate(" + (x(d.start_date.valueOf()) + yAxisLeftOffset) + "," + y(d.task_id) + ")";
  };

  function tickFormater(d) {
    // We can't use d3.time.format as that uses local time, so instead we use
    // moment as that handles our "global" timezone.
    return moment(d).strftime(tickFormat);
  }

  var x = d3.time.scale().domain([timeDomainStart, timeDomainEnd]).range([0, width - yAxisLeftOffset]).clamp(true);
  var y = d3.scale.ordinal().domain(taskTypes).rangeRoundBands([0, height - margin.top - margin.bottom], .1);
  var xAxis = d3.svg.axis().scale(x).orient("bottom").tickFormat(tickFormater).tickSubdivide(true).tickSize(8).tickPadding(8);
  var yAxis = d3.svg.axis().scale(y).orient("left").tickSize(0);

  var initTimeDomain = function (tasks) {
    if (timeDomainMode === FIT_TIME_DOMAIN_MODE) {
      if (tasks === undefined || tasks.length < 1) {
        timeDomainStart = d3.time.day.offset(new Date(), -3);
        timeDomainEnd = d3.time.hour.offset(new Date(), +3);
        return;
      }

      tasks.forEach(a => {
        if (!(a.start_date instanceof moment)) {
          a.start_date = moment(a.start_date);
        }

        if (!(a.end_date instanceof moment)) {
          a.end_date = moment(a.end_date);
        }
      });
      timeDomainEnd = moment.max(tasks.map(a => a.end_date)).valueOf();
      timeDomainStart = moment.min(tasks.map(a => a.start_date)).valueOf();
    }
  };

  var initAxis = function () {
    x = d3.time.scale().domain([timeDomainStart, timeDomainEnd]).range([0, width - yAxisLeftOffset]).clamp(true);
    y = d3.scale.ordinal().domain(taskTypes).rangeRoundBands([0, height - margin.top - margin.bottom], .1);
    xAxis = d3.svg.axis().scale(x).orient("bottom").tickFormat(tickFormater).tickSubdivide(true).tickSize(8).tickPadding(8);
    yAxis = d3.svg.axis().scale(y).orient("left").tickSize(0);
  };

  function gantt(tasks) {
    initTimeDomain(tasks);
    initAxis();
    var svg = d3.select(selector).append("svg").attr("class", "chart").attr("width", width + margin.left + margin.right).attr("height", height + margin.top + margin.bottom).append("g").attr("class", "gantt-chart").attr("width", width + margin.left + margin.right).attr("height", height + margin.top + margin.bottom).attr("transform", "translate(" + margin.left + ", " + margin.top + ")");
    svg.selectAll(".chart").data(tasks, keyFunction).enter().append("rect").on('mouseover', tip.show).on('mouseout', tip.hide).on('click', function (d) {
      call_modal(d.task_id, d.execution_date, d.extraLinks);
    }).attr("class", function (d) {
      return d.state || "null";
    }).attr("y", 0).attr("transform", rectTransform).attr("height", function (d) {
      return y.rangeBand();
    }).attr("width", function (d) {
      return d3.max([x(d.end_date.valueOf()) - x(d.start_date.valueOf()), 1]);
    });
    svg.append("g").attr("class", "x axis").attr("transform", "translate(" + yAxisLeftOffset + ", " + (height - margin.top - margin.bottom) + ")").transition().call(xAxis);
    svg.append("g").attr("class", "y axis").transition().attr("transform", "translate(" + yAxisLeftOffset + ", 0)").call(yAxis);
    svg.call(tip);
    return gantt;
  }

  ;

  gantt.redraw = function (tasks) {
    initTimeDomain(tasks);
    initAxis();
    var svg = d3.select(".chart");
    var ganttChartGroup = svg.select(".gantt-chart");
    var rect = ganttChartGroup.selectAll("rect").data(tasks, keyFunction);
    rect.enter().insert("rect", ":first-child").attr("rx", 5).attr("ry", 5).attr("class", function (d) {
      return d.state || "null";
    }).transition().attr("y", 0).attr("transform", rectTransform).attr("height", function (d) {
      return y.rangeBand();
    }).attr("width", function (d) {
      return d3.max([x(d.end_date.valueOf()) - x(d.start_date.valueOf()), 1]);
    });
    rect.exit().remove();
    svg.select(".x").transition().call(xAxis);
    svg.select(".y").transition().call(yAxis);
    return gantt;
  };

  gantt.margin = function (value) {
    if (!arguments.length) return margin;
    margin = value;
    return gantt;
  };

  gantt.timeDomain = function (value) {
    if (!arguments.length) return [timeDomainStart, timeDomainEnd];
    timeDomainStart = +value[0], timeDomainEnd = +value[1];
    return gantt;
  };
  /**
   * @param {string}
   *                vale The value can be "fit" - the domain fits the data or
   *                "fixed" - fixed domain.
   */


  gantt.timeDomainMode = function (value) {
    if (!arguments.length) return timeDomainMode;
    timeDomainMode = value;
    return gantt;
  };

  gantt.taskTypes = function (value) {
    if (!arguments.length) return taskTypes;
    taskTypes = value;
    return gantt;
  };

  gantt.width = function (value) {
    if (!arguments.length) return width;
    width = +value;
    return gantt;
  };

  gantt.height = function (value) {
    if (!arguments.length) return height;
    height = +value;
    return gantt;
  };

  gantt.tickFormat = function (value) {
    if (!arguments.length) return tickFormat;
    tickFormat = value;
    return gantt;
  };

  gantt.selector = function (value) {
    if (!arguments.length) return selector;
    selector = value;
    return gantt;
  };

  return gantt;
};

/***/ })

/******/ });
});