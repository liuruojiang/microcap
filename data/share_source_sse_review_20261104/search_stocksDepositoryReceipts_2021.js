/**
 * 股票总览-股票列表
 */

/*
@param { Object } item filter遍历的每一项
*/

function filtersArrDate(item) {
  if (this.currentDay == "day") {
    return item.DATE_TYPE == "D"; // 日
  } else if (this.currentDay == "month") {
    return item.DATE_TYPE == "M"; // 月
  } else if (this.currentDay == "year") {
    return item.DATE_TYPE == "Y"; // 年
  } else if (this.currentDay == "other") {
    // return item.DATE_TYPE == '' // 上市以来其他逻辑进行处理
  }
}

/**
 * @author JackCHEN
 * @method transformPara
 * @version 创建时间${date} ${2022-03-15}
 * @description 仅支持对象转换不支持复杂参数转换
 * @param { Object } obj
 * @returns { String } str
 */

function transformPara(obj) {
  var newObj = JSON.parse(JSON.stringify(obj));
  var str;
  for (var i = 0; i < Object.keys(newObj).length; i++) {
    if (i == 0) {
      str = Object.keys(newObj)[i] + "=" + newObj[Object.keys(newObj)[i]];
    } else {
      str +=
        "&" + Object.keys(newObj)[i] + "=" + newObj[Object.keys(newObj)[i]];
    }
  }
  return str;
}

// 时间格式化
function dateReplace(date) {
  return date
    ? date.substring(0, 4) +
    "-" +
    date.substring(4, 6) +
    "-" +
    date.substring(6)
    : "-";
}

function dateFormat(date) {
  return date
    ? date.substring(0, 4) +
    "年" +
    date.substring(4, 6) +
    "月" +
    date.substring(6, 8) +
    "日"
    : "-";
}
var stockHome = {
  listCompanyUrl:
    "/disclosure/listedinfo/announcement/json/stock_bulletin_publish_order.json", //信息披露-最新公告
  homeOverviewList: {
    STOCK_TYPE: "1", // ‘1’为A股，’2’为B股，‘8’为科创板
    REG_PROVINCE: "", // 注册地省级代码
    CSRC_CODE: "", // CSRC门类行业代码
    STOCK_CODE: "", // 股票代码
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
    COMPANY_STATUS: "2,4,5,7,8",
    type: "inParams",
    isPagination: true,
    "pageHelp.cacheSize": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.pageSize": 10,
    "pageHelp.pageNo": 1,
  },
  init: function () {
    this.loadEvents();
    this.getStaticRecent();
  },
  loadEvents: function () {
    var _this = this;
    _this.getHomeOverviewList();
    // 点击搜索、回车触发查询
    triggerSearch(_this.inputCodeOpen);
  },
  //上市公司静态公告
  getStaticRecent: function () {
    var _this = this;
    $.getJSON(_this.listCompanyUrl + "?v=" + Math.random(), function (data) {
      if (data && data.publishData) {
        var liHtm = "";
        $.each(data.publishData, function (k, v) {
          if (k < 5) {
            liHtm +=
              '<li class="list-group-item d-flex justify-content-between">';
            liHtm +=
              '<a href="' +
              (v.bulletinUrl ? v.bulletinUrl : "") +
              '" target="_blank">' +
              (v.bulletinClassic == "M_LC_HOLDER_BULLETIN" ||
                v.bulletinClassic == "K_LC_HOLDER_BULLETIN"
                ? "<em>股东自行披露</em>"
                : "") +
              (v.bulletinTitle ? v.bulletinTitle : "") +
              "</a>";
            liHtm +=
              '<span class="text-nowrap new_date">' +
              (v.discloseDate ? v.discloseDate : "") +
              "</span>";
            liHtm += "</li>";
          }
        });
        $(".sse_colContent .overview-list").html(liHtm);
      }
    });
  },
  // 股票列表代码输入框跳转
  inputCodeOpen: function () {
    var _this = stockHome;
    var inputCode = $("#inputCode").val() ? $("#inputCode").val() : "";
    var res = /^\d{6}$/;
    if (inputCode == "") {
      bootstrapModal({
        text: "请输入证券代码",
      });
      return;
    } else if (inputCode && !res.test(inputCode)) {
      bootstrapModal({
        text: "证券代码必须为6位数字",
      });
      return;
    }
    // 搜索内容添加cookie
    popularize.saveCpyIntoCookie(
      $("#inputCode").val().trim(),
      $("#inputCode"),
      1
    );
    window.open(
      "/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=" +
      inputCode,
      "_blank"
    );
  },
  getHomeOverviewList: function () {
    var _this = this;
    // tableData_1260
    var emptyTr = '<tr><td colspan="4">暂无数据</td></tr>';
    var stockHomeHtml = "<thead><tr>";
    stockHomeHtml += '<th class="text-nowrap">证券代码</th>';
    stockHomeHtml += '<th class="text-nowrap">证券简称</th>';
    stockHomeHtml += '<th class="text-nowrap">扩位证券简称</th>';
    stockHomeHtml += '<th class="text-nowrap">上市公司全称</th>';
    stockHomeHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: sseQueryURL + "sseQuery/commonQuery.do",
      data: _this.homeOverviewList,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            stockHomeHtml += "<tr>";
            stockHomeHtml +=
              '<td class="text-nowrap"><a href="/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=' +
              v.COMPANY_CODE +
              '" target="_blank">' +
              v.A_STOCK_CODE +
              "</td>"; // 证券代码
            stockHomeHtml +=
              '<td class="text-nowrap"><span>' + v.SEC_NAME_CN + "</span></td>"; // 证券简称
            stockHomeHtml +=
              '<td class="text-nowrap"><span>' +
              v.SEC_NAME_FULL +
              "</span></td>"; // 扩位证券简称
            stockHomeHtml += '<td class="text-nowrap">' + v.FULL_NAME + "</td>"; // 上市公司全称
            stockHomeHtml += "</tr>";
          });
          stockHomeHtml += "</tbody>";
          $(".js_stockHome .table").html(stockHomeHtml);
        } else {
          stockHomeHtml += emptyTr;
          stockHomeHtml += "</tbody>";
          $(".js_stockHome .table").html(stockHomeHtml);
        }
      },
    });
  },
};
if ($(".js_stockHome").length > 0) {
  stockHome.init();
}
// 股票总览-股票列表 END

/**
 * 股票总览-地区行业分类
 */
var stockHomeAreaTrade = {
  stockHomeAreaTradeParam: {
    sqlId: "COMMON_SSE_CP_GPJCTPZ_DQHYFL_DQFL_L",
    isPagination: true,
    "pageHelp.pageSize": 10,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 1,
  },
  init: function () {
    this.loadEvents();
  },
  loadEvents: function () {
    var _this = this;
    _this.getStockHomeAreaTradeList();
  },
  getStockHomeAreaTradeList: function () {
    var _this = this;
    var emptyTr = '<tr><td colspan="4">暂无数据</td></tr>';
    var stockHomeAreaTradeHtml = "<thead><tr>";
    stockHomeAreaTradeHtml += '<th class="text-nowrap">地区</th>';
    stockHomeAreaTradeHtml += '<th class="text-nowrap">仅发A股</th>';
    stockHomeAreaTradeHtml += '<th class="text-nowrap">A、B股</th>';
    stockHomeAreaTradeHtml += '<th class="text-nowrap">仅发B股</th>';
    stockHomeAreaTradeHtml += "</tr></thead><tbody>";

    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: sseQueryURL + "commonQuery.do?jsonCallBack=?",
      data: _this.stockHomeAreaTradeParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            stockHomeAreaTradeHtml += "<tr>";
            stockHomeAreaTradeHtml +=
              '<td class="text-nowrap"><a href="/assortment/stock/areatrade/area/detail.shtml?AREANAME=' +
              v.AREA +
              '" target="_blank">' +
              v.CODE_DESC +
              "</td>"; // 地区
            stockHomeAreaTradeHtml +=
              '<td class="text-nowrap">' + v.ASHARENUM + "</td>"; // 仅发A股
            stockHomeAreaTradeHtml +=
              '<td class="text-nowrap">' + v.ABSHARENUM + "</td>"; // A、B股
            stockHomeAreaTradeHtml +=
              '<td class="text-nowrap">' + v.BSHARENUM + "</td>"; // 仅发B股
            stockHomeAreaTradeHtml += "</tr>";
          });
          stockHomeAreaTradeHtml += "</tbody>";
          $(".js_stockHomeAreaTrade .table").html(stockHomeAreaTradeHtml);
        } else {
          stockHomeAreaTradeHtml += emptyTr;
          stockHomeAreaTradeHtml += "</tbody>";
          $(".js_stockHomeAreaTrade .table").html(stockHomeAreaTradeHtml);
        }
      },
    });
  },
};
if ($(".js_stockHomeAreaTrade").length > 0) {
  stockHomeAreaTrade.init();
}
// 股票总览-地区行业分类 END

/**
 * 股票总览-成交概况
 */
var stockHomeOverview = {
  stockHomeOverviewParam: {
    sqlId: "COMMON_SSE_SJ_GPSJ_CJGK_MRGK_C",
    PRODUCT_CODE: "17",
    isPagination: true,
    "pageHelp.pageSize": 6,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 1,
  },
  init: function () {
    this.loadEvents();
  },
  loadEvents: function () {
    var _this = this;
    _this.getStockHomeOverviewList();
  },
  getStockHomeOverviewList: function () {
    var _this = this;
    var arrList = [];
    var emptyTr = '<tr><td colspan="2">暂无数据</td></tr>';
    var stockHomeOverviewHtml = "<thead><tr>";
    stockHomeOverviewHtml += '<th class="text-nowrap">单日情况</th>';
    stockHomeOverviewHtml += '<th class="text-right">股票总体</th>';
    stockHomeOverviewHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: sseQueryURL + "commonQuery.do?jsonCallBack=?",
      data: _this.stockHomeOverviewParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          _this.arrList = [
            ["市价总值(亿元)", stringFormatterTwo(data.result[0].TOTAL_VALUE)],
            ["流通市值(亿元)", stringFormatterTwo(data.result[0].NEGO_VALUE)],
            ["成交量(亿股)", stringFormatterTwo(data.result[0].TRADE_VOL)],
            ["成交金额(亿元)", stringFormatterTwo(data.result[0].TRADE_AMT)],
            ["平均市盈率(倍)", stringFormatterTwo(data.result[0].AVG_PE_RATE)],
          ];
          $.each(_this.arrList, function (k, v) {
            stockHomeOverviewHtml += "<tr>";
            stockHomeOverviewHtml +=
              '<td class="text-nowrap">' + v[0] + "</td>"; // 单日情况
            stockHomeOverviewHtml += '<td class="text-right">' + v[1] + "</td>"; // 股票总体
            stockHomeOverviewHtml += "</tr>";
          });

          stockHomeOverviewHtml += "</tbody>";
          $(".js_stockHomeOverview .table").html(stockHomeOverviewHtml);
        } else {
          stockHomeOverviewHtml += emptyTr;
          stockHomeOverviewHtml += "</tbody>";
          $(".js_stockHomeOverview .table").html(stockHomeOverviewHtml);
        }
      },
    });
  },
};
if ($(".js_stockHomeOverview").length > 0) {
  stockHomeOverview.init();
}
// 股票总览-成交概况 END

/**
 * 股票总览-股票市价总值排名
 */
var stockHomeValue = {
  stockHomeParam: {
    sqlId: "COMMON_SSE_SJ_SZPM_L",
    LIST_BOARD: "1",
    TOTAL_VALUE_DESC: "1",
    isPagination: true,
    "pageHelp.pageSize": 5,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 1,
  },
  init: function () {
    this.loadEvents();
  },
  loadEvents: function () {
    var _this = this;
    _this.getStockHomeValueHtmlList();
  },
  getStockHomeValueHtmlList: function () {
    var _this = this;
    var emptyTr = '<tr><td colspan="3">暂无数据</td></tr>';
    var stockHomeValueHtml = "<thead><tr>";
    stockHomeValueHtml += '<th class="text-nowrap">名次</th>';
    stockHomeValueHtml += '<th class="codeNameWidth">股票代码</th>';
    stockHomeValueHtml += '<th class="codeNameWidth">股票简称</th>';
    stockHomeValueHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: sseQueryURL + "commonQuery.do?jsonCallBack=?",
      data: _this.stockHomeParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            stockHomeValueHtml += "<tr>";
            stockHomeValueHtml += '<td class="text-nowrap">' + v.RN + "</td>"; // 名次
            stockHomeValueHtml +=
              '<td class="codeNameWidth"><a href="/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=' +
              v.SEC_CODE +
              '" target="_blank">' +
              v.SEC_CODE +
              "</td>"; // 股票代码
            stockHomeValueHtml +=
              '<td class="codeNameWidth"><span>' + v.SEC_NAME + "</span></td>"; // 股票简称
            stockHomeValueHtml += "</tr>";
          });
          stockHomeValueHtml += "</tbody>";
          $(".js_stockHomeValue .table").html(stockHomeValueHtml);
        } else {
          stockHomeValueHtml += emptyTr;
          stockHomeValueHtml += "</tbody>";
          $(".js_stockHomeValue .table").html(stockHomeValueHtml);
        }
      },
    });
  },
};
if ($(".js_stockHomeValue").length > 0) {
  stockHomeValue.init();
}
// 股票总览-股票市价总值排名 END

/**
 * 股票总览-活跃股排名
 */
var stockHomeLively = {
  stockHomeLiveParam: {
    sqlId: "COMMON_SSE_SJ_GPSJ_HYGPM_L",
    LIST_BOARD: "1",
    TRADE_VOL_DESC: "1",
    isPagination: true,
    "pageHelp.pageSize": 5,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 1,
  },
  init: function () {
    this.loadEvents();
  },
  loadEvents: function () {
    var _this = this;
    _this.getStockHomeLivelyList();
  },
  getStockHomeLivelyList: function () {
    var _this = this;
    var emptyTr = '<tr><td colspan="3">暂无数据</td></tr>';
    var stockHomeLivelyHtml = "<thead><tr>";
    stockHomeLivelyHtml += '<th class="text-nowrap">名次</th>';
    stockHomeLivelyHtml += '<th class="codeNameWidth">股票代码</th>';
    stockHomeLivelyHtml += '<th class="codeNameWidth">股票简称</th>';
    stockHomeLivelyHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: sseQueryURL + "commonQuery.do?jsonCallBack=?",
      data: _this.stockHomeLiveParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            stockHomeLivelyHtml += "<tr>";
            stockHomeLivelyHtml += '<td class="text-nowrap">' + v.RN + "</td>"; // 名次
            stockHomeLivelyHtml +=
              '<td class="codeNameWidth"><a href="/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=' +
              v.SEC_CODE +
              '" target="_blank">' +
              v.SEC_CODE +
              "</td>"; // 股票代码
            stockHomeLivelyHtml +=
              '<td class="codeNameWidth"><span>' + v.SEC_NAME + "</span></td>"; // 股票简称
            stockHomeLivelyHtml += "</tr>";
          });
          stockHomeLivelyHtml += "</tbody>";
          $(".js_stockHomeLively .table").html(stockHomeLivelyHtml);
        } else {
          stockHomeLivelyHtml += emptyTr;
          stockHomeLivelyHtml += "</tbody>";
          $(".js_stockHomeLively .table").html(stockHomeLivelyHtml);
        }
      },
    });
  },
};
if ($(".js_stockHomeLively").length > 0) {
  stockHomeLively.init();
}
// 股票总览-活跃股排名 END

/* 股票 */
var stocksDepository = {
  industryUrl: sseQueryURL + "commonQuery.do", // 科创板行业url
  industryParam: {
    sqlId: "COMMON_SSE_KCBZL_MARKETDATA_INDUSTRIES_L",
  },
  stockUrl: sseQueryURL + "sseQuery/commonQuery.do", // 默认
  stockParam: {
    STOCK_TYPE: "1", // ‘1’为A股，’2’为B股，‘8’为科创板
    REG_PROVINCE: "", // 注册地省级代码
    CSRC_CODE: "", // CSRC门类行业代码
    STOCK_CODE: "", // 股票代码
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
    COMPANY_STATUS: "2,4,5,7,8",
    type: "inParams",
    isPagination: true,
    "pageHelp.cacheSize": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.pageSize": 25,
    "pageHelp.pageNo": 1,
  },
  plateArr: [
    {
      name: "主板A股",
      value: "1",
    },
    {
      name: "主板B股",
      value: "2",
    },
    {
      name: "科创板",
      value: "8",
    },
  ],
  isKCB: false, // 是否是科创板
  industryArr: [
    // 主板行业数据
    {
      name: "请选择行业",
      value: "-",
    },
    {
      name: "A.农、林、牧、渔业",
      value: "A",
    },
    {
      name: "B.采掘业",
      value: "B",
    },
    {
      name: "C.制造业",
      value: "C",
    },
    {
      name: "D.电力、热力、燃气及水生产和供应业",
      value: "D",
    },
    {
      name: "E.建筑业",
      value: "E",
    },
    {
      name: "F.批发和零售业",
      value: "F",
    },
    {
      name: "G.交通运输、仓储和邮政业",
      value: "G",
    },
    {
      name: "H.住宿和餐饮业",
      value: "H",
    },
    {
      name: "I.信息传输、软件和信息技术服务业",
      value: "I",
    },
    {
      name: "J.金融业",
      value: "J",
    },
    {
      name: "K.房地产业",
      value: "K",
    },
    {
      name: "L.租赁和商务服务业",
      value: "L",
    },
    {
      name: "M.科学研究和技术服务业",
      value: "M",
    },
    {
      name: "N.水利、环境和公共设施管理业",
      value: "N",
    },
    {
      name: "O.居民服务、修理和其他服务业",
      value: "O",
    },
    {
      name: "P.教育",
      value: "P",
    },
    {
      name: "Q.卫生和社会工作",
      value: "Q",
    },
    {
      name: "R.文化、体育和娱乐业",
      value: "R",
    },
    {
      name: "S.综合",
      value: "S",
    },
  ],
  regionArr: [
    // 主板地区数据
    {
      name: "请选择地区",
      value: "-",
    },
    {
      name: "安徽",
      value: "340000",
    },
    {
      name: "北京",
      value: "110000",
    },
    {
      name: "黑龙江",
      value: "230000",
    },
    {
      name: "湖北",
      value: "420000",
    },
    {
      name: "湖南",
      value: "430000",
    },
    {
      name: "吉林",
      value: "220000",
    },
    {
      name: "江苏",
      value: "320000",
    },
    {
      name: "江西",
      value: "360000",
    },
    {
      name: "辽宁",
      value: "210000",
    },
    {
      name: "内蒙古",
      value: "150000",
    },
    {
      name: "宁夏",
      value: "640000",
    },
    {
      name: "青海",
      value: "630000",
    },
    {
      name: "福建",
      value: "350000",
    },
    {
      name: "山东",
      value: "370000",
    },
    {
      name: "山西",
      value: "140000",
    },
    {
      name: "陕西",
      value: "610000",
    },
    {
      name: "上海",
      value: "310000",
    },
    {
      name: "四川",
      value: "510000",
    },
    {
      name: "天津",
      value: "120000",
    },
    {
      name: "西藏",
      value: "540000",
    },
    {
      name: "新疆",
      value: "650000",
    },
    {
      name: "云南",
      value: "530000",
    },
    {
      name: "浙江",
      value: "330000",
    },
    {
      name: "甘肃",
      value: "620000",
    },
    {
      name: "重庆",
      value: "500000",
    },
    {
      name: "广东",
      value: "440000",
    },
    {
      name: "广西",
      value: "450000",
    },
    {
      name: "贵州",
      value: "520000",
    },
    {
      name: "海南",
      value: "460000",
    },
    {
      name: "河北",
      value: "130000",
    },
    {
      name: "河南",
      value: "410000",
    },
  ],
  kcbIndustryArr: "<option selected='selected' value='-'>请选择行业</option>",
  downLoad: sseQueryURL + "sseQuery/commonExcelDd.do?",
  codeHref: "/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=",
  detailHref: "/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=",
  init: function () {
    this.loadEvents();
    this.getDownHref();
    this.getIndustry(); // 行业--科创板
    this.showStockList(1); // 默认股票列表
  },
  // 下拉绑定、默认加载
  loadEvents: function () {
    var _this = this;
    // 添加下载链接
    var stockHref =
      '<a class="tableDownload js_download-export" href="">下载</a>';
    $(".js_stocksDepository .title_lev2").append(stockHref);
    // 下拉框数据渲染
    bootstrapSelect({
      method: function () {
        /** 板块数据渲染**/
        $(".js_plate .selectpicker")
          .html(addSelectOption(_this.plateArr))
          .selectpicker("refresh")
          .selectpicker("render");
        // 下拉框改变触发查询
        $(".js_plate .selectpicker").on("changed.bs.select", function (e) {
          var plate = e.target.value;
          if (plate == "8") {
            // 行业数据渲染--科创板
            $(".js_industry .selectpicker")
              .html(_this.kcbIndustryArr)
              .selectpicker("refresh")
              .selectpicker("render");
            _this.isKCB = true;
          } else {
            // 行业数据渲染--主板
            if (_this.isKCB) {
              $(".js_industry .selectpicker")
                .html(addSelectOption(_this.industryArr))
                .selectpicker("refresh")
                .selectpicker("render");
            }
            _this.isKCB = false;
          }
          _this.setCodeParam(); // 条件查询股票列表
        });

        /** 地区渲染**/
        $(".js_city .selectpicker")
          .html(addSelectOption(_this.regionArr))
          .selectpicker("refresh")
          .selectpicker("render");
        // 下拉框改变触发查询
        $(".js_city .selectpicker").on("changed.bs.select", function (e) {
          _this.setCodeParam(); // 条件查询股票列表
        });

        /** 行业渲染**/
        $(".js_industry .selectpicker").html(addSelectOption(_this.industryArr))
          .selectpicker("refresh")
          .selectpicker("render");
        // 下拉框改变触发查询
        $(".js_industry .selectpicker").on("changed.bs.select", function (e) {
          _this.setCodeParam(); // 条件查询股票列表
        });
      },
    });
    triggerSearch(_this.setCodeParam); // 点击搜索、回车触发查询
  },
  // 设置参数
  setCodeParam: function () {
    var _this = stocksDepository;
    var inputCode = $("#inputCode").val();
    var res = /^\d{6}$/;
    if (inputCode && !res.test(inputCode)) {
      bootstrapModal({
        text: "证券代码必须为6位数字",
      });
      return;
    }
    _this.stockParam.CSRC_CODE =
      $(".js_industry .selectpicker").val() &&
        $(".js_industry .selectpicker").val() != "-"
        ? $(".js_industry .selectpicker").val()
        : "";
    _this.stockParam.REG_PROVINCE =
      $(".js_city .selectpicker").val() &&
        $(".js_city .selectpicker").val() != "-"
        ? $(".js_city .selectpicker").val()
        : "";
    _this.stockParam.STOCK_TYPE =
      $(".js_plate .selectpicker").val() &&
        $(".js_plate .selectpicker").val() != "-"
        ? $(".js_plate .selectpicker").val()
        : "";
    _this.stockParam.STOCK_CODE = inputCode;
    _this.stockUrl = _this.stockUrl; // 条件查询url
    _this.getDownHref(); // 改变下载地址
    _this.showStockList(1);
  },
  // 改变下载链接地址
  getDownHref: function () {
    var paramObj = {
      sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
      type: "inParams",
      CSRC_CODE: this.stockParam.CSRC_CODE,
      STOCK_CODE: this.stockParam.STOCK_CODE,
      REG_PROVINCE: this.stockParam.REG_PROVINCE,
      STOCK_TYPE: this.stockParam.STOCK_TYPE,
      COMPANY_STATUS: this.stockParam.COMPANY_STATUS,
    };
    var stockHref = this.downLoad + transformPara(paramObj);
    $(".js_stocksDepository .js_download-export").attr("href", stockHref);
  },
  // 获取科创板行业
  getIndustry: function () {
    var _this = this;
    getJSONP({
      type: "post",
      url: _this.industryUrl,
      data: _this.industryParam,
      successCallback: function (data) {
        if (data && data.result) {
          $.each(data.result, function (k, v) {
            _this.kcbIndustryArr +=
              '<option value="' +
              v.SMALL_CLASS_NAME +
              '">' +
              v.SMALL_CLASS_NAME +
              "</option>";
          });
        }
      },
    });
  },
  // 获取股票列表
  showStockList: function (pageIndex) {
    var _this = stocksDepository;
    if (!paginationChange(_this.stockParam, pageIndex)) {
      // 触发分页改变分页参数
      return;
    }
    var htm =
      "<thead><tr><th>证券代码</th><th>证券简称</th><th>扩位证券简称</th><th>上市日期</th></tr></thead><tbody>";
    var emptyTr = '<tr><td colspan="18">暂无数据</tr>';
    getJSONP({
      type: "post",
      url: _this.stockUrl,
      data: _this.stockParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          if (_this.stockParam.STOCK_TYPE != "8") {
            $.each(data.result, function (k, v) {
              htm += "<tr>";
              htm +=
                '<td class="text-nowrap"><a href="' +
                _this.codeHref +
                v.COMPANY_CODE +
                '" target="_blank">' +
                (_this.stockParam.STOCK_TYPE != "2"
                  ? v.A_STOCK_CODE
                  : v.B_STOCK_CODE) +
                "</a></td>";
              htm += // 主板简称后加标识
                '<td class="text-nowrap">' +
                v.SEC_NAME_CN +
                kcbShowIconFlag(v.PRODUCT_STATUS) +
                "</span></td>";
              htm +=
                '<td class="text-nowrap">' +
                v.SEC_NAME_FULL + //扩位证券简称
                "</td>";
              htm +=
                '<td class="text-nowrap">' + dateReplace(v.LIST_DATE) + "</td>";
              htm += "</tr>";
            });
            Page.navigation(
              ".js_stocksDepository .pagination-box",
              data.pageHelp.pageCount,
              data.pageHelp.total,
              data.pageHelp.pageNo,
              data.pageHelp.pageSize,
              "stocksDepository.showStockList"
            );
            htm += "</tbody>";
            $(".js_stocksDepository .table").html(htm);
          } else {
            $.each(data.result, function (k, v) {
              var iconStr = kcbShowIconFlag(v.PRODUCT_STATUS);
              htm += "<tr>";
              htm +=
                '<td class="text-nowrap"><a href="' +
                _this.codeHref +
                v.COMPANY_CODE +
                '" target="_blank">' +
                (data.stockType != 2 ? v.A_STOCK_CODE : v.B_STOCK_CODE) +
                "</a></td>";

              htm +=
                '<td class="text-nowrap">' +
                v.SEC_NAME_CN +
                iconStr +
                "</span></td>";
              htm +=
                '<td class="text-nowrap">' +
                v.SEC_NAME_FULL + //扩位证券简称
                "</td>";
              htm +=
                '<td class="text-nowrap">' + dateReplace(v.LIST_DATE) + "</td>";
              htm += "</tr>";
            });
            Page.navigation(
              ".js_stocksDepository .pagination-box",
              data.pageHelp.pageCount,
              data.pageHelp.total,
              data.pageHelp.pageNo,
              data.pageHelp.pageSize,
              "stocksDepository.showStockList"
            );
            htm += "</tbody>";
            $(".js_stocksDepository .table").html(htm);
          }
        } else {
          htm += emptyTr;
          htm += "</tbody>";
          $(".js_stocksDepository .table").html(htm);
          $(".js_stocksDepository .pagination-box").html("");
        }
        // 搜索代码添加cookie
        popularize.saveCpyIntoCookie(
          $("#inputCode").val().trim(),
          $("#inputCode"),
          1
        );
      },
      errCallback: function () {
        htm += emptyTr;
        htm += "</tbody>";
        $(".js_stocksDepository .table").html(htm);
        $(".js_stocksDepository .pagination-box").html("");
      },
    });
  },
};
if ($(".js_stocksDepository").length > 0) {
  stocksDepository.init();
}

/* 股票 end */

/* 首次发行待上市 */
var initialOffering = {
  stockUrl: sseQueryURL + "security/stock/getStockListData2.do", // 默认
  stockUrl1: sseQueryURL + "security/stock/getStockListData.do", // 条件查询
  stockParam: {
    isPagination: true,
    stockCode: "",
    csrcCode: "",
    areaName: "",
    stockType: 3,
    "pageHelp.cacheSize": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.pageSize": 25,
    "pageHelp.pageNo": 1,
  },
  downLoad: sseQueryURL + "security/stock/downloadStockListFile.do?",
  codeHref: "/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=",
  detailHref: "/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=",
  init: function () {
    this.loadEvents();
    this.getDownHref();
    this.showStockList(1);
  },
  // 下拉绑定、默认加载
  loadEvents: function () {
    var _this = this;
    // 添加下载链接
    var stockHref =
      '<a class="tableDownload js_download-export" href="">下载</a>';
    $(".js_initialOffering .title_lev2").append(stockHref);
    // 下拉框数据渲染
    bootstrapSelect({
      method: function () {
        /** 地区渲染**/
        $(".js_city .selectpicker")
          .selectpicker("refresh")
          .selectpicker("render");
        // 下拉框改变触发查询
        $(".js_city .selectpicker").on("changed.bs.select", function (e) {
          _this.setCodeParam(); // 条件查询首次发行待上市列表
        });

        /** 行业渲染**/
        $(".js_industry .selectpicker").html(addSelectOption(_this.industryArr))
          .selectpicker("refresh")
          .selectpicker("render");
        // 下拉框改变触发查询
        $(".js_industry .selectpicker").on("changed.bs.select", function (e) {
          _this.setCodeParam(); // 条件查询首次发行待上市列表
        });
      },
    });
    triggerSearch(_this.setCodeParam); // 点击搜索、回车触发查询
  },
  // 设置参数
  setCodeParam: function () {
    var _this = initialOffering;
    var inputCode = $("#inputCode").val();
    var res = /^\d{6}$/;
    if (inputCode && !res.test(inputCode)) {
      bootstrapModal({
        text: "证券代码必须为6位数字",
      });
      return;
    }
    _this.stockParam.stockCode = inputCode;
    _this.stockParam.areaName = $(".js_city .selectpicker").val();
    _this.stockParam.csrcCode = $(".js_industry .selectpicker").val();
    _this.stockUrl = _this.stockUrl1; // 条件查询url
    _this.getDownHref(); // 改变下载地址
    _this.showStockList(1);
  },
  // 改变下载链接地址
  getDownHref: function () {
    var stockHref =
      this.downLoad +
      "csrcCode=" +
      this.stockParam.csrcCode +
      "&stockCode=" +
      this.stockParam.stockCode +
      "&areaName=" +
      this.stockParam.areaName +
      "&stockType=" +
      this.stockParam.stockType;
    $(".js_initialOffering .js_download-export").attr("href", stockHref);
  },
  // 获取首次发行待上市列表
  showStockList: function (pageIndex) {
    var _this = this;
    if (!paginationChange(_this.stockParam, pageIndex)) {
      // 触发分页改变分页参数
      return;
    }
    var htm =
      "<thead><tr><th>公司代码</th><th>公司简称</th><th>上市日期</th><th>公告</th></tr></thead><tbody>";
    var emptyTr = '<tr><td colspan="18">暂无数据</tr>';
    getJSONP({
      type: "post",
      url: _this.stockUrl,
      data: _this.stockParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            htm += "<tr>";
            htm +=
              '<td class="text-nowrap"><a href="' +
              _this.codeHref +
              v.COMPANY_CODE +
              '" target="_blank">' +
              v.COMPANY_CODE +
              "</a></td>";
            htm += '<td class="text-nowrap">' + v.COMPANY_ABBR + "</td>";
            htm += '<td class="text-nowrap">' + v.LISTING_DATE + "</td>";
            htm +=
              '<td class="text-nowrap"><a href="' +
              _this.detailHref +
              v.COMPANY_CODE +
              '" target="_blank">详情</a></td>';
            htm += "</tr>";
          });
          Page.navigation(
            ".js_initialOffering .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "initialOffering.showStockList"
          );
          htm += "</tbody>";
          $(".js_initialOffering .table").html(htm);
        } else {
          htm += emptyTr;
          htm += "</tbody>";
          $(".js_initialOffering .table").html(htm);
          $(".js_initialOffering .pagination-box").html("");
        }
        // 搜索代码添加cookie
        popularize.saveCpyIntoCookie(
          $("#inputCode").val().trim(),
          $("#inputCode"),
          1
        );
      },
      errCallback: function () {
        htm += emptyTr;
        htm += "</tbody>";
        $(".js_initialOffering .table").html(htm);
        $(".js_initialOffering .pagination-box").html("");
      },
    });
  },
};
if ($(".js_initialOffering").length > 0) {
  initialOffering.init();
}

/* 首次发行待上市 end */

/* 暂停/终止上市公司 */
var suspensionTermination = {
  industryUrl: sseQueryURL + "commonQuery.do", //科创板行业url
  industryParam: {
    sqlId: "COMMON_SSE_KCBZL_MARKETDATA_INDUSTRIES_L",
  },
  stockUrl: sseQueryURL + "commonQuery.do", //默认
  stockParam: {
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
    isPagination: true,
    STOCK_CODE: "",
    CSRC_CODE: "",
    REG_PROVINCE: "",
    STOCK_TYPE: "1,2",
    COMPANY_STATUS: "5",
    type: "inParams",
    "pageHelp.cacheSize": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.pageSize": 25,
    "pageHelp.pageNo": 1,
  },
  listedArr: [
    {
      name: "暂停上市",
      value: "5",
    },
    {
      name: "终止上市",
      value: "3",
    },
  ],
  plateArr: [
    {
      name: "主板",
      value: "1,2",
    },
    {
      name: "科创板",
      value: "8",
    },
  ],
  industryArr: [
    // 主板行业数据
    {
      name: "请选择行业",
      value: "-",
    },
    {
      name: "A.农、林、牧、渔业",
      value: "A",
    },
    {
      name: "B.采掘业",
      value: "B",
    },
    {
      name: "C.制造业",
      value: "C",
    },
    {
      name: "D.电力、热力、燃气及水生产和供应业",
      value: "D",
    },
    {
      name: "E.建筑业",
      value: "E",
    },
    {
      name: "F.批发和零售业",
      value: "F",
    },
    {
      name: "G.交通运输、仓储和邮政业",
      value: "G",
    },
    {
      name: "H.住宿和餐饮业",
      value: "H",
    },
    {
      name: "I.信息传输、软件和信息技术服务业",
      value: "I",
    },
    {
      name: "J.金融业",
      value: "J",
    },
    {
      name: "K.房地产业",
      value: "K",
    },
    {
      name: "L.租赁和商务服务业",
      value: "L",
    },
    {
      name: "M.科学研究和技术服务业",
      value: "M",
    },
    {
      name: "N.水利、环境和公共设施管理业",
      value: "N",
    },
    {
      name: "O.居民服务、修理和其他服务业",
      value: "O",
    },
    {
      name: "P.教育",
      value: "P",
    },
    {
      name: "Q.卫生和社会工作",
      value: "Q",
    },
    {
      name: "R.文化、体育和娱乐业",
      value: "R",
    },
    {
      name: "S.综合",
      value: "S",
    },
  ],
  areaArr: [
    {
      name: "请选择地区",
      value: "-",
    },
    {
      name: "安徽",
      value: "340000",
    },
    {
      name: "北京",
      value: "110000",
    },
    {
      name: "黑龙江",
      value: "230000",
    },
    {
      name: "湖北",
      value: "420000",
    },
    {
      name: "湖南",
      value: "430000",
    },
    {
      name: "吉林",
      value: "220000",
    },
    {
      name: "江苏",
      value: "320000",
    },
    {
      name: "江西",
      value: "360000",
    },
    {
      name: "辽宁",
      value: "210000",
    },
    {
      name: "内蒙古",
      value: "150000",
    },
    {
      name: "宁夏",
      value: "640000",
    },
    {
      name: "青海",
      value: "630000",
    },
    {
      name: "福建",
      value: "350000",
    },
    {
      name: "山东",
      value: "370000",
    },
    {
      name: "山西",
      value: "140000",
    },
    {
      name: "陕西",
      value: "610000",
    },
    {
      name: "上海",
      value: "310000",
    },
    {
      name: "四川",
      value: "510000",
    },
    {
      name: "天津",
      value: "120000",
    },
    {
      name: "西藏",
      value: "540000",
    },
    {
      name: "新疆",
      value: "650000",
    },
    {
      name: "云南",
      value: "530000",
    },
    {
      name: "浙江",
      value: "330000",
    },
    {
      name: "甘肃",
      value: "620000",
    },
    {
      name: "重庆",
      value: "500000",
    },
    {
      name: "广东",
      value: "440000",
    },
    {
      name: "广西",
      value: "450000",
    },
    {
      name: "贵州",
      value: "520000",
    },
    {
      name: "海南",
      value: "460000",
    },
    {
      name: "河北",
      value: "130000",
    },
    {
      name: "河南",
      value: "410000",
    },
  ],
  kcbIndustryArr: "<option selected='selected' value='-'>请选择行业</option>",
  downLoad: sseQueryURL + "/sseQuery/commonExcelDd.do?",
  codeHref: "/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=",
  detailHref: "/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=",
  init: function () {
    this.loadEvents();
    this.getDownHref();
    this.getIndustry(); //行业--科创板
    this.showStockList(1); //默认暂停/终止上市列表列表
  },
  //下拉绑定、默认加载
  loadEvents: function () {
    var _this = this;
    //添加下载链接
    var stockHref =
      '<a class="tableDownload js_download-export" href="">下载</a>';
    $(".js_suspensionTermination .title_lev2").append(stockHref);
    //下拉框数据渲染
    bootstrapSelect({
      method: function () {
        /**暂停/终止上市渲染**/
        $(".js_listed .selectpicker")
          .html(addSelectOption(_this.listedArr))
          .selectpicker("refresh")
          .selectpicker("render");
        //下拉框改变触发查询
        $(".js_listed .selectpicker").on("changed.bs.select", function (e) {
          _this.setCodeParam(e.target.value); //条件查询股票列表
        });
        /**板块数据渲染**/
        $(".js_plate .selectpicker")
          .html(addSelectOption(_this.plateArr))
          .selectpicker("refresh")
          .selectpicker("render");
        //下拉框改变触发查询
        $(".js_plate .selectpicker").on("changed.bs.select", function (e) {
          var plate = e.target.value;
          if (plate == "1,2") {
            $(".js_industry .selectpicker")
              .html(addSelectOption(_this.industryArr))
              .selectpicker("refresh")
              .selectpicker("render");
          }
          if (plate == "8") {
            //行业数据渲染--科创板
            $(".js_industry .selectpicker")
              .html(_this.kcbIndustryArr)
              .selectpicker("refresh")
              .selectpicker("render");
          }
          _this.setCodeParam(); //条件查询股票列表
        });

        /**地区渲染**/
        $(".js_city .selectpicker")
          .html(addSelectOption(_this.areaArr))
          .selectpicker("refresh")
          .selectpicker("render");
        //下拉框改变触发查询
        $(".js_city .selectpicker").on("changed.bs.select", function (e) {
          _this.setCodeParam(); //条件查询股票列表
        });

        /**行业渲染**/
        $(".js_industry .selectpicker").html(addSelectOption(_this.industryArr))
          .selectpicker("refresh")
          .selectpicker("render");
        //下拉框改变触发查询
        $(".js_industry .selectpicker").on("changed.bs.select", function (e) {
          _this.setCodeParam(); //条件查询股票列表
          _this.getDownHref(); //改变下载地址
        });
      },
    });
    triggerSearch(_this.setCodeParam); //点击搜索、回车触发查询
  },
  //设置参数
  setCodeParam: function (listNum) {
    var _this = suspensionTermination;
    var inputCode = $("#inputCode").val();
    var res = /^\d{6}$/;
    if (inputCode && !res.test(inputCode)) {
      bootstrapModal({
        text: "证券代码必须为6位数字",
      });
      return;
    }
    _this.stockParam.STOCK_CODE = inputCode;
    _this.stockParam.STOCK_TYPE = $(".js_plate .selectpicker").val();
    _this.stockParam.REG_PROVINCE =
      $(".js_city .selectpicker").val() != "-"
        ? $(".js_city .selectpicker").val()
        : "";
    _this.stockParam.CSRC_CODE =
      $(".js_industry .selectpicker").val() != "-"
        ? $(".js_industry .selectpicker").val()
        : "";
    _this.stockParam.COMPANY_STATUS =
      $(".js_listed .selectpicker").val() != "-"
        ? $(".js_listed .selectpicker").val()
        : "";
    if (listNum) {
      if (
        !(
          _this.stockParam.STOCK_CODE ||
          _this.stockParam.CSRC_CODE ||
          _this.stockParam.REG_PROVINCE
        )
      ) {
        _this.stockParam.STOCK_CODE = inputCode;
        _this.stockParam.STOCK_TYPE = $(".js_plate .selectpicker").val();
        _this.stockParam.REG_PROVINCE =
          $(".js_city .selectpicker").val() != "-"
            ? $(".js_city .selectpicker").val()
            : "";
        _this.stockParam.CSRC_CODE =
          $(".js_industry .selectpicker").val() != "-"
            ? $(".js_industry .selectpicker").val()
            : "";
        _this.stockParam.COMPANY_STATUS =
          $(".js_listed .selectpicker").val() != "-"
            ? $(".js_listed .selectpicker").val()
            : "";
      }
      _this.getDownHref(); //改变下载地址
      _this.showStockList(1);
    } else {
      _this.stockParam.STOCK_CODE = inputCode;
      _this.stockParam.STOCK_TYPE = $(".js_plate .selectpicker").val();
      _this.stockParam.REG_PROVINCE =
        $(".js_city .selectpicker").val() != "-"
          ? $(".js_city .selectpicker").val()
          : "";
      _this.stockParam.CSRC_CODE =
        $(".js_industry .selectpicker").val() != "-"
          ? $(".js_industry .selectpicker").val()
          : "";
      _this.stockParam.COMPANY_STATUS =
        $(".js_listed .selectpicker").val() != "-"
          ? $(".js_listed .selectpicker").val()
          : "";
      if (
        _this.stockParam.STOCK_CODE ||
        _this.stockParam.CSRC_CODE ||
        _this.stockParam.REG_PROVINCE
      ) {
        _this.getDownHref(); //改变下载地址
        _this.showStockList(1);
      } else {
        bootstrapModal({
          text: "请输入查询条件后再查询",
        });
      }
    }
  },
  //改变下载链接地址
  getDownHref: function () {
    var paramObj = {
      sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L", //搁置不用
      type: "inParams",
      CSRC_CODE: this.stockParam.CSRC_CODE,
      STOCK_CODE: this.stockParam.STOCK_CODE,
      REG_PROVINCE: this.stockParam.REG_PROVINCE,
      STOCK_TYPE: this.stockParam.STOCK_TYPE,
      COMPANY_STATUS: this.stockParam.COMPANY_STATUS,
    };
    var val = $(".js_listed .filter-option-inner-inner").html();
    if (val == "终止上市") {
      paramObj.sqlId = "COMMON_SSE_CP_GPJCTPZ_GPLB_ZZGP_L"; //接口替换
    } else {
      paramObj.sqlId = "COMMON_SSE_CP_GPJCTPZ_GPLB_ZTGP_L"; //接口替换
    }
    var stockHref = this.downLoad + transformPara(paramObj);
    $(".js_suspensionTermination .js_download-export").attr("href", stockHref);
  },
  //获取科创板行业
  getIndustry: function () {
    var _this = this;
    getJSONP({
      type: "post",
      url: _this.industryUrl,
      data: _this.industryParam,
      successCallback: function (data) {
        if (data && data.result) {
          $.each(data.result, function (k, v) {
            _this.kcbIndustryArr +=
              '<option value="' +
              v.SMALL_CLASS_NAME +
              '">' +
              v.SMALL_CLASS_NAME +
              "</option>";
          });
        }
      },
    });
  },
  //获取暂停/终止上市列表
  showStockList: function (pageIndex) {
    var _this = this;
    if (!paginationChange(_this.stockParam, pageIndex)) {
      //触发分页改变分页参数
      return;
    }
    var type = _this.stockParam.STOCK_TYPE;
    var listedType = _this.stockParam.COMPANY_STATUS;
    var htm = "";
    if (listedType == 5) {
      htm =
        "<thead><tr><th>公司代码</th><th>公司简称</th><th>上市日期</th><th>暂停上市日期</th></tr></thead><tbody>";
    } else {
      htm =
        "<thead><tr><th>原公司代码</th><th>原公司简称</th><th>上市日期</th><th>终止上市日期</th></tr></thead><tbody>";
    }
    var emptyTr = '<tr><td colspan="18">暂无数据</tr>';
    getJSONP({
      type: "post",
      url: _this.stockUrl,
      data: _this.stockParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            htm += "<tr>";
            htm +=
              '<td class="codeNameWidth"><a href="' +
              _this.codeHref +
              v.COMPANY_CODE +
              '" target="_blank">' +
              v.COMPANY_CODE +
              "</a></td>";
            htm +=
              '<td class="codeNameWidth"><span>' +
              v.COMPANY_ABBR +
              "</span></td>";
            htm +=
              '<td class="text-nowrap">' +
              v.LIST_DATE.replace(/^(\d{4})(\d{2})(\d{2})$/, "$1-$2-$3") +
              "</td>";
            if (listedType == 5) {
              htm += '<td class="text-nowrap">' + "-" + "</td>"; // 暂停上市日期
            }
            if (listedType == 3) {
              htm +=
                "<td>" +
                v.DELIST_DATE.replace(/^(\d{4})(\d{2})(\d{2})$/, "$1-$2-$3") +
                "</td>"; // 终止上市日期
            }
            htm += "</tr>";
          });
          Page.navigation(
            ".js_suspensionTermination .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "suspensionTermination.showStockList"
          );
          htm += "</tbody>";
          $(".js_suspensionTermination .table").html(htm);
        } else {
          htm += emptyTr;
          htm += "</tbody>";
          $(".js_suspensionTermination .table").html(htm);
          $(".js_suspensionTermination .pagination-box").html("");
        }
        //搜索代码添加cookie
        popularize.saveCpyIntoCookie(
          $("#inputCode").val().trim(),
          $("#inputCode"),
          1
        );
      },
      errCallback: function () {
        htm += emptyTr;
        htm += "</tbody>";
        $(".js_suspensionTermination .table").html(htm);
        $(".js_suspensionTermination .pagination-box").html("");
      },
    });
  },
};
if ($(".js_suspensionTermination").length > 0) {
  suspensionTermination.init();
}
/* 暂停/终止上市公司 end */

/* 全球存托凭证信息 */
var depositoryReceipt = {
  isHome: true,
  commonUrl: sseQueryURL + "commonQuery.do",
  depositoryParam: {
    security_code: "",
    isPagination: true,
    sqlId: "COMMON_SSE_HLT_GDR_CTPZ_L",
    "pageHelp.pageSize": 5,
    "pageHelp.cacheSize": 1,
  },
  toHref: "https://www.londonstockexchange.com/home/homepage.htm",
  detailHref: "/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=",
  moreHref: "/assortment/stock/slsc/gdr/info/detail/index.shtml?GDR_CODE=",
  init: function () {
    this.loadEvents();
  },
  loadEvents: function () {
    $("#inputCode").attr("placeholder", "证券代码");
    $("#inputCode").attr("maxlength", "25");
    triggerSearch(this.setCodeParam); // 证券代码enter,点击查询
    if (!($(".js_depositoryReceipt .title_lev2").length > 0)) {
      this.isHome = false;
      this.depositoryParam["security_code"] = getWindowHref().GDR_CODE
        ? decodeURI(getWindowHref().GDR_CODE)
        : "";
      $("#inputCode").val(this.depositoryParam["security_code"]);
      this.depositoryParam["isPagination"] = true;
      this.depositoryParam["pageHelp.pageSize"] = 25;
    }
    this.depositoryList(1);
  },
  // 设置参数
  setCodeParam: function () {
    var _this = depositoryReceipt;
    var inputCode = $("#inputCode").val();
    _this.depositoryParam.security_code = inputCode;
    if (_this.isHome) {
      location.href =
        "/assortment/stock/slsc/gdr/info/?GDR_CODE=" +
        _this.depositoryParam.security_code;
    } else {
      _this.depositoryList(1);
    }
  },
  depositoryList: function (pageIndex) {
    var _this = this;
    if (!_this.isHome) {
      if (!paginationChange(_this.depositoryParam, pageIndex)) {
        // 触发分页改变分页参数
        return;
      }
    }
    var htm =
      "<thead><tr><th>GDR代码</th><th>GDR全称</th><th>GDR首次上市日期</th><th>GDR上市地</th><th><span>A股代码</span></th><th><span>A股简称</span></th><th>公告</th><th>转换比例</th><th>更多</th></tr></thead><tbody>";
    var emptyTr = '<tr><td colspan="18">暂无数据</tr>';
    getJSONP({
      type: "post",
      url: _this.commonUrl,
      data: _this.depositoryParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            if ((_this.isHome && k < 5) || !_this.isHome) {
              htm += "<tr>";
              htm += '<td class="text-nowrap">' + v.GDR_SEC_CODE + "</td>";
              htm += '<td class="text-nowrap">' + v.GDR_SEC_ABBR + "</td>";
              htm += '<td class="text-nowrap">' + v.GDR_LIST_DATE + "</td>";
              htm +=
              '<td class="text-nowrap"><span>' +
              v.GDR_LOC +
              "</span></td>";
              htm +=
                '<td class="text-nowrap"><span>' +
                v.BASIC_SEC_CODE +
                "</span></td>";
              htm +=
                '<td class="text-nowrap"><span>' +
                v.BASIC_SEC_ABBR +
                "</span></td>";
              htm +=
                '<td class="text-nowrap"><a target="_blank" href="' +
                _this.detailHref +
                v.BASIC_SEC_CODE +
                '">详情</a></td>';
              htm += '<td class="text-nowrap">' + v.ZHBL + "</td>";
              htm +=
                '<td class="text-nowrap"><a target="_blank" href="' +
                _this.moreHref +
                v.GDR_SEC_CODE +
                '">详情</a></td>';
              htm += "</tr>";
            }
            if (!_this.isHome) {
              Page.navigation(
                ".js_depositoryReceipt .pagination-box",
                data.pageHelp.pageCount,
                data.pageHelp.total,
                data.pageHelp.pageNo,
                data.pageHelp.pageSize,
                "depositoryReceipt.depositoryList"
              );
            }
          });
          htm += "</tbody>";
          $(".js_depositoryReceipt .table").html(htm);
        } else {
          htm += emptyTr;
          htm += "</tbody>";
          $(".js_depositoryReceipt .table").html(htm);
          $(".js_depositoryReceipt .pagination-box").html("");
        }
        if (_this.depositoryParam.security_code) {
          // 搜索代码添加cookie
          popularize.saveCpyIntoCookie(
            $("#inputCode").val().trim(),
            $("#inputCode"),
            1
          );
        }
      },
      errCallback: function () {
        htm += emptyTr;
        htm += "</tbody>";
        $(".js_depositoryReceipt .table").html(htm);
        $(".js_depositoryReceipt .pagination-box").html("");
      },
    });
  },
};
if ($(".js_depositoryReceipt").length > 0) {
  depositoryReceipt.init();
}

/* 全球存托凭证信息 end */

/* 全球存托凭证信息详情 */
var depositoryReceiptDetail = {
  commonUrl: sseQueryURL + "commonQuery.do",
  detailParam: {
    GDR_SEC_CODE: "",
    isPagination: true,
    sqlId: "COMMON_SSE_HLT_GDR_CTPZ_C",
    "pageHelp.pageSize": 25,
    "pageHelp.cacheSize": 1,
  },
  init: function () {
    this.detailParam.GDR_SEC_CODE = getWindowHref().GDR_CODE
      ? decodeURI(getWindowHref().GDR_CODE)
      : "";
    this.getDetail(1);
  },
  // 加载详情列表
  getDetail: function (pageIndex) {
    var _this = this;
    if (!paginationChange(_this.detailParam, pageIndex)) {
      // 触发分页改变分页参数
      return;
    }
    var htm = "<thead><tr>";
    htm += '<th class="text-nowrap">GDR代码</th>';
    htm += '<th class="text-nowrap">GDR全称</th>';
    htm += '<th class="text-nowrap">GDR上市日期</th>';
    htm += '<th class="text-nowrap text-right">本次对应的GDR份额数</th>';
    htm += '<th class="text-nowrap text-right">本次上市的基础股票数</th>';
    htm += '<th class="text-nowrap">新增股份的上市日期</th>';
    htm += '<th class="text-nowrap">A股代码</th>';
    htm += '<th class="text-nowrap">A股简称</th>';
    htm += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      url: _this.commonUrl,
      data: _this.detailParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            if (k == 0) {
              $(".js_depositoryReceiptDetail .title_lev2").text(
                v.BASIC_SEC_CODE +
                " " +
                v.BASIC_SEC_ABBR +
                " 全球存托凭证发行信息"
              );
            }
            htm += "<tr>";
            htm += '<td class="text-nowrap">' + v.GDR_SEC_CODE + "</td>";
            htm += '<td class="text-nowrap">' + v.GDR_SEC_ABBR + "</td>";
            htm += '<td class="text-nowrap">' + v.GDR_LIST_DATE + "</td>";
            htm += '<td class="text-nowrap text-right">' + v.GDR_VOL + "</td>";
            htm +=
              '<td class="text-nowrap text-right">' + v.BASIC_VOL + "</td>";
            htm += '<td class="text-nowrap">' + v.ADD_LIST_DATE + "</td>";
            htm += '<td class="text-nowrap">' + v.BASIC_SEC_CODE + "</td>";
            htm += '<td class="text-nowrap">' + v.BASIC_SEC_ABBR + "</td>";
            htm += "</tr>";
          });
          Page.navigation(
            ".js_depositoryReceiptDetail .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "depositoryReceiptDetail.getDetail"
          );
        } else {
          htm += '<tr><td colspan="18">暂无数据</td></tr>';
          $(".js_depositoryReceiptDetail .pagination-box").html("");
        }
        htm += "</tbody>";
        $(".js_depositoryReceiptDetail .table").html(htm);
      },
      errCallback: function () {
        htm += '<tr><td colspan="18">暂无数据</td></tr>';
        htm += "</tbody>";
        $(".js_depositoryReceiptDetail .table").html(htm);
        $(".js_depositoryReceiptDetail .pagination-box").html("");
      },
    });
  },
};
if ($(".js_depositoryReceiptDetail").length > 0) {
  depositoryReceiptDetail.init();
}

/* 全球存托凭证信息详情 end */

/* 跨境转换机构 */
var crossBorderOrgan = {
  commonUrl: sseQueryURL + "commonSoaQuery.do?",
  crossBorderParam: {
    sqlId: "CP_GPYCTPZ_HLHTCTPZ_QQCTPZ_JWKJZHJG",
  },
  init: function () {
    this.getOrganList();
  },
  getOrganList: function () {
    var _this = this;
    var htm = '<div class="table-responsive sse_table_T01"><div class="table_inline_wrap"><div class="table_inline_nooddbg_wrap"><table class="table"><tbody><tr><th>境外跨境转换机构名称</th><th>机构英文名称</th><th>可开展跨境转换业务的境外市场范围</th><th>市场范围英文名称</th></tr>'
    $('.table-responsive ').html(htm)
    var emptyTr = '<tr><td colspan="18">暂无数据</tr>';
    getJSONP({
      type: "post",
      url: _this.commonUrl,
      data: _this.crossBorderParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          var institutionEnList = []; // 数组暂存进行后续比较
          var len = 0; // 相同跨行数
          for(var i = 0; i < data.result.length; i++){
              if (len == 0 || data.result[i].institutionCn == institutionEnList[len- 1].institutionCn) {
                institutionEnList[len++] = data.result[i];
              } else{
                  htm += "<tr>";   
                  htm += '<td  rowspan=' + len + '>' + institutionEnList[0].institutionCn + "</td>";
                  htm += '<td rowspan=' + len + '>'  + institutionEnList[0].institutionEn + "</td>";
                  
                for (var j = 0; j < len; j++){
                  htm = j == 0 ? htm : htm + "<tr>";
                  htm += '<td>' + institutionEnList[j].exchangeCn + "</td>";
                  htm += '<td>' + institutionEnList[j].exchangeEn + "</td>";
                  htm += "</tr>";
                }
                institutionEnList = [];
                len = 0;
                institutionEnList[len++] = data.result[i];
              }
          }
          if (len > 0) {
            htm += "<tr>";   
            htm += '<td  rowspan=' + len + '>' + institutionEnList[0].institutionCn + "</td>";
            htm += '<td rowspan=' + len + '>'  + institutionEnList[0].institutionEn + "</td>";
            
          for (var j = 0; j < len; j++){
            htm = j == 0 ? htm : htm + "<tr>";
            htm += '<td>' + institutionEnList[j].exchangeCn + "</td>";
            htm += '<td>' + institutionEnList[j].exchangeEn + "</td>";
            htm += "</tr>";
            }
          }
          htm += "</tbody></table></div></div>";
          $(".js_crossBorderOrgan .table").html(htm);
          $(".js_crossBorderOrgan .new_date").text(
            "数据日期：" + turnDateAddLine(data.result[length - 1].fileDate)
          );
        } else {
          htm += emptyTr;
          htm += "</tbody></table></div></div>";
          $(".js_crossBorderOrgan .table").html(htm);
        }
      },
      errCallback: function () {
        htm += emptyTr;
        htm += "</tbody></table></div></div>";
        $(".js_crossBorderOrgan .table").html(htm);
      },
    })
  }
}
if ($(".js_crossBorderOrgan").length > 0) {
  crossBorderOrgan.init();
}
/* 跨境转换机构 end */

/* 优先股 */
var preferredStock = {
  preferredUrl: sseQueryURL + "commonSoaQuery.do?",
  preferredParam: {
    isPagination: false,
    sqlId: "PREFERENCE_BOND_LB",
    token: "QUERY",
  },
  toHref: "/assortment/stock/preferreds/preferredDetail/index.shtml?",
  init: function () {
    this.showPreferredList();
  },
  showPreferredList: function () {
    var _this = this;
    var htm =
      "<thead><tr><th>简称</th><th>上市/挂牌日期</th></tr></thead><tbody>";
    getJSONP({
      type: "post",
      url: _this.preferredUrl,
      data: _this.preferredParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            htm += "<tr>";
            htm +=
              '<td><a href="' +
              _this.toHref +
              "stockCode=" +
              v.stockCode +
              "&preferStockCode=" +
              v.preferStockCode +
              '" target="_blank">' +
              v.preferStockName +
              "</a></td>";
            htm +=
              "<td>" +
              (v.listedDate
                ? v.listedDate.substring(0, 4) +
                "年" +
                v.listedDate.substring(4, 6) +
                "月" +
                v.listedDate.substring(6, 8) +
                "日"
                : "") +
              "</td>";
            htm += "</tr>";
          });
        } else {
          htm += '<tr><td colspan="18">暂无数据</td></tr>';
        }
        htm += "</tbody>";
        $(".js_preferredStock .table").html(htm);
      },
      errCallback: function () {
        htm += '<tr><td colspan="18">暂无数据</td></tr>';
        htm += "</tbody>";
        $(".js_preferredStock .table").html(htm);
      },
    });
  },
};
if ($(".js_preferredStock").length > 0) {
  preferredStock.init();
}

/* 优先股 end*/

/* 优先股详情 */
var preferredStockDetail = {
  preferredUrl: sseQueryURL + "commonSoaQuery.do?",
  preferredParam: {
    isPagination: false,
    sqlId: "PREFERENCE_BOND_LB",
    token: "QUERY",
    stockCode: "",
    preferStockCode: "",
  },
  init: function () {
    this.preferredParam.stockCode = getWindowHref().stockCode
      ? decodeURI(getWindowHref().stockCode)
      : "";
    this.preferredParam.preferStockCode = getWindowHref().preferStockCode
      ? decodeURI(getWindowHref().preferStockCode)
      : "";
    this.showPreferredDetail();
  },
  showPreferredDetail: function () {
    var _this = this;
    var htm = "<tbody>";
    getJSONP({
      type: "post",
      url: _this.preferredUrl,
      data: _this.preferredParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          var res = data.result[0];
          $(".js_preferredStockDetail .title_lev2").text(
            res.stockAbbrName + "(" + res.stockCode + ")"
          );
          htm +=
            '<tr><td class="codeNameWidth">公司简称</td><td>' +
            res.stockAbbrName +
            "</td></tr>";
          htm +=
            '<tr><td class="codeNameWidth">优先股代码</td><td>' +
            res.preferStockCode +
            "</td></tr>";
          htm +=
            '<tr><td class="codeNameWidth">优先股简称</td><td>' +
            res.preferStockName +
            "</td></tr>";
          htm += "<tr><td>发行人全称</td><td>" + res.companyName + "</td></tr>";
          htm +=
            '<tr><td class="codeNameWidth">发行人公司代码</td><td>' +
            res.stockCode +
            "</td></tr>";
          htm +=
            "<tr><td>发行方式</td><td>" +
            (res.issueType == ""
              ? res.issueType
              : res.issueType == 0
                ? "非公开"
                : "公开") +
            "</td></tr>";
          htm +=
            "<tr><td>发行价格（单位:元）</td><td>" +
            res.stockPrice +
            "</td></tr>";
          htm +=
            "<tr><td>发行数量（单位:万股）</td><td>" +
            res.stockNum +
            "</td></tr>";
          htm +=
            "<tr><td>发行日期</td><td>" +
            _this.formatDate(res.issueDate) +
            "</td></tr>";
          htm +=
            "<tr><td>上市/挂牌日期</td><td>" +
            _this.formatDate(res.listedDate) +
            "</td></tr>";
          htm +=
            "<tr><td>每股面值（单位:元）</td><td>" +
            res.parValue +
            "</td></tr>";
          htm +=
            "<tr><td>初始票面股息率</td><td>" + res.dividendRate + "</td></tr>";
          htm +=
            '<tr><td>股息类型</td><td class="table_titlewrap">' +
            res.dividendType +
            "</td></tr>";
          htm +=
            "<tr><td>股息说明</td><td>" + res.dividendComments + "</td></tr>";
          htm +=
            "<tr><td>每年股息支付次数</td><td>" +
            res.payFrequency +
            "</td></tr>";
          htm +=
            '<tr><td class="text-nowrap">股东大会是否有权取消股息支付</td><td>' +
            (res.isVotecancel == ""
              ? res.isVotecancel
              : res.isVotecancel == 0
                ? "否"
                : "是") +
            "</td></tr>";
          htm +=
            "<tr><td>股息是否可累积</td><td>" +
            (res.isAccumulate == ""
              ? res.isAccumulate
              : res.isAccumulate == 0
                ? "否"
                : "是") +
            "</td></tr>";
          htm +=
            "<tr><td>是否可回售</td><td>" +
            (res.isPutable == ""
              ? res.isPutable
              : res.isPutable == 0
                ? "否"
                : "是") +
            "</td></tr>";
          htm += "<tr><td>回售条款</td><td>" + res.putProvision + "</td></tr>";
          htm +=
            "<tr><td>是否可赎回</td><td>" +
            (res.isCallback == ""
              ? res.isCallback
              : res.isCallback == 0
                ? "否"
                : "是") +
            "</td></tr>";
          htm +=
            '<tr><td>赎回条款</td><td class="table_titlewrap">' +
            res.callProvision +
            "</td></tr>";
          htm +=
            "<tr><td>是否有权参与剩余利润分配</td><td>" +
            (res.isParticipate == ""
              ? res.isParticipate
              : res.isParticipate == 0
                ? "否"
                : "是") +
            "</td></tr>";
          htm +=
            "<tr><td>是否可转换为普通股</td><td>" +
            (res.isConvertible == ""
              ? res.isConvertible
              : res.isConvertible == 0
                ? "否"
                : "是") +
            "</td></tr>";
          htm +=
            "<tr><td>计入权益</td><td>" +
            (res.isEquityCredit == ""
              ? res.isEquityCredit
              : res.isEquityCredit == 0
                ? "否"
                : "是") +
            "</td></tr>";
          htm += "<tr><td>其他</td><td>" + res.other + "</td></tr>";
        } else {
          htm += '<tr><td colspan="18">暂无数据</td></tr>';
        }
        htm += "</tbody>";
        $(".js_preferredStockDetail .table").html(htm);
      },
      errCallback: function () {
        htm += '<tr><td colspan="18">暂无数据</td></tr>';
        htm += "</tbody>";
        $(".js_preferredStockDetail .table").html(htm);
      },
    });
  },
  formatDate: function (date) {
    if (null != date && date != "") {
      var year = date.substring(0, 4);
      var month = date.substring(4, 6);
      var day = date.substring(6, 8);
      return year + "年" + month + "月" + day + "日";
    }
  },
};
if ($(".js_preferredStockDetail").length > 0) {
  preferredStockDetail.init();
}

/* 优先股详情 end*/

/* 地区分类*/
var areaClassification = {
  // 行业分类-地区分类
  areaParams: {
    isPagination: true,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_DQHYFL_DQFL_L",
  },
  dqDate: "-",
  toHref: "/assortment/stock/areatrade/area/detail.shtml?AREANAME=", // 代码跳转地址
  areaArr: tableData["tableData_1250"],
  areaUrl: sseQueryURL + "commonQuery.do?",
  init: function () {
    $(".js_areaClassification").append(
      '<div class="remarks">点击地区，可以查看每个地区业的股票列表</div>'
    );
    this.showAreaList();
  },
  showAreaList: function () {
    var _this = areaClassification;
    var areaHtm = "<thead><tr>";
    areaHtm += '<th class="text-left">地区</th>';
    areaHtm += '<th class="text-right">仅发A股</th>';
    areaHtm += '<th class="text-right">A、B股</th>';
    areaHtm += '<th class="text-right">仅发B股</th>';
    areaHtm += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      url: _this.areaUrl,
      data: _this.areaParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          dqDate = data.result[0].FILE_DATE;
          $.each(data.result, function (k, v) {
            areaHtm += "<tr>";
            areaHtm +=
              '<td class="text-nowrap text-left"><a target="_blank" href="' +
              _this.toHref +
              v.AREA +
              '">' +
              v.CODE_DESC +
              "</a></td>";
            areaHtm += '<td class="text-right">' + v.ASHARENUM + "</td>";
            areaHtm +=
              '<td class="text-nowrap text-right">' + v.ABSHARENUM + "</td>";
            areaHtm +=
              '<td class="text-nowrap text-right">' + v.BSHARENUM + "</td>";
            areaHtm += "</tr>";
            areaHtm += "</tbody>";
          });
          $(".js_areaClassification .table").html(areaHtm);
          $(".js_areaClassification .new_date").text(
            "数据日期：" + dqDate.replace(/^(\d{4})(\d{2})(\d{2})$/, "$1-$2-$3")
          );
        } else {
          areaHtm += '<tr><td colspan="18">暂无数据</td></tr>';
        }
      },
    });
    $(".js_areaClassification .table").html(areaHtm);
  },
};
if ($(".js_areaClassification").length > 0) {
  areaClassification.init();
}

/* 地区分类 end*/

/* 地区分类详情 */
var areaDetail = {
  commonUrl: sseQueryURL + "commonQuery.do",
  areaParam: {
    isPagination: false,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_DQHYFL_XQ_L",
    REG_PROVINCE: "",
  },
  areaOption: [
    {
      name: "安徽",
      value: "340000",
    },
    {
      name: "内蒙古",
      value: "150000",
    },
    {
      name: "宁夏",
      value: "640000",
    },
    {
      name: "青海",
      value: "630000",
    },
    {
      name: "福建",
      value: "350000",
    },
    {
      name: "山东",
      value: "370000",
    },
    {
      name: "山西",
      value: "140000",
    },
    {
      name: "陕西",
      value: "610000",
    },
    {
      name: "上海",
      value: "310000",
    },
    {
      name: "四川",
      value: "510000",
    },
    {
      name: "天津",
      value: "120000",
    },
    {
      name: "北京",
      value: "110000",
    },
    {
      name: "西藏",
      value: "540000",
    },
    {
      name: "新疆",
      value: "650000",
    },
    {
      name: "云南",
      value: "530000",
    },
    {
      name: "浙江",
      value: "330000",
    },
    {
      name: "甘肃",
      value: "620000",
    },
    {
      name: "重庆",
      value: "500000",
    },
    {
      name: "广东",
      value: "440000",
    },
    {
      name: "广西",
      value: "450000",
    },
    {
      name: "贵州",
      value: "520000",
    },
    {
      name: "海南",
      value: "460000",
    },
    {
      name: "黑龙江",
      value: "230000",
    },
    {
      name: "河北",
      value: "130000",
    },
    {
      name: "河南",
      value: "410000",
    },
    {
      name: "湖北",
      value: "420000",
    },
    {
      name: "湖南",
      value: "430000",
    },
    {
      name: "吉林",
      value: "220000",
    },
    {
      name: "江苏",
      value: "320000",
    },
    {
      name: "江西",
      value: "360000",
    },
    {
      name: "辽宁",
      value: "210000",
    },
    {
      name: "境外",
      value: "980000",
    },
  ],
  toHref: "/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=", // 代码跳转地址
  init: function () {
    this.areaParam.REG_PROVINCE = getWindowHref().AREANAME
      ? decodeURI(getWindowHref().AREANAME)
      : "";
    this.loadEvents();
  },
  loadEvents: function () {
    var _this = this;
    // 地区下拉框数据渲染
    bootstrapSelect({
      method: function () {
        $(".js_area .selectpicker").selectpicker({
          noneSelectedText: "请选择地区", // 默认显示内容
        });
        // 地区数据渲染
        $(".js_area .selectpicker")
          .html(addSelectOption(_this.areaOption))
          .selectpicker("refresh")
          .selectpicker("render");
        // 下拉框初始化完成后
        $(".js_area .selectpicker").on("loaded.bs.select", function (e) {
          if (e.target.value == _this.areaParam.REG_PROVINCE) {
            _this.getStockList();
          } else {
            $(".js_area .selectpicker").selectpicker(
              "val",
              _this.areaParam.REG_PROVINCE
            );
          }
        });
        // 下拉框改变触发查询
        $(".js_area .selectpicker").on("changed.bs.select", function (e) {
          _this.areaParam.REG_PROVINCE = e.target.value;
          _this.getStockList();
        });
      },
    });
  },
  getStockList: function () {
    var _this = areaDetail;
    var htm =
      "<thead><tr><th>上市公司代码</th><th>上市公司名称</th><th>A股代码</th><th>B股代码</th></tr></thead><tbody>";
    getJSONP({
      type: "post",
      url: _this.commonUrl,
      data: _this.areaParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            htm += "<tr>";
            htm +=
              '<td class="text-nowrap"><a target="_blank" href="' +
              _this.toHref +
              v.COMPANY_CODE +
              '">' +
              v.COMPANY_CODE +
              "</a></td>";
            htm += '<td class="text-nowrap">' + v.FULL_NAME + "</td>";
            htm += '<td class="text-nowrap">' + v.A_STOCK_CODE + "</td>";
            htm += '<td class="text-nowrap">' + v.B_STOCK_CODE + "</td>";
            htm += "</tr>";
          });
          $(".js_areaDetail .title_lev2").html(data.result[0].AREADESC);
        } else {
          htm += '<tr><td colspan="18">暂无数据</td></tr>';
        }
        htm += "</tbody>";
        $(".js_areaDetail .table").html(htm);
      },
      errCallback: function () {
        htm += '<tr><td colspan="18">暂无数据</td></tr>';
        htm += "</tbody>";
        $(".js_areaDetail .table").html(htm);
      },
    });
  },
};
if ($(".js_areaDetail").length > 0) {
  areaDetail.init();
}

/* 地区分类详情 end*/

/* 行业分类*/
var tradeClassification = {
  productParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_DQHYFL_HYFL_L",
  },
  productUrl: sseQueryURL + "commonQuery.do?",
  tradeArr: tableData["tableData_1258"],
  tradeDate: "-",
  init: function () {
    this.showTradeList();
  },
  showTradeList: function () {
    var _this = tradeClassification;
    var htm = "<thead><tr>";
    htm += "<th>行业名称</th>";
    htm += "<th>行业代码</th>";
    htm += '<th class="text-right">股票数(只)</th>';
    htm += '<th class="text-right">市价总值(元)</th>';
    htm += '<th class="text-right">平均市盈率</th>';
    htm += '<th class="text-right">平均价格(元)</th>';
    htm += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      url: _this.productUrl,
      data: _this.productParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          tradeDate = data.result[0].TRADE_DATE;
          $.each(data.result, function (k, v) {
            htm += "<tr>";
            htm +=
              '<td class="text-nowrap"><a target="_blank" href="/assortment/stock/areatrade/trade/detail.shtml?csrcCode=' +
              v.CSRC_CODE +
              "&csrcName=" +
              v.CSRC_NAME +
              '">' +
              v.CSRC_NAME +
              "</a></td>";
            htm += '<td class="text-nowrap">' + v.CSRC_CODE + "</td>";
            htm += '<td class="text-nowrap text-right">' + v.LIST_NUM + "</td>";
            htm +=
              '<td class="text-nowrap text-right">' + v.TOTAL_VALUE + "</td>";
            htm +=
              '<td class="text-nowrap text-right">' + v.AVG_PE_RATE + "</td>";
            htm +=
              '<td class="text-nowrap text-right">' + v.AVG_PRICE + "</td>";
            htm += "</tr>";
          });
          $(".js_tradeClassification .new_date").text(
            "数据日期：" +
            tradeDate.replace(/^(\d{4})(\d{2})(\d{2})$/, "$1-$2-$3")
          );
        } else {
          htm += '<tr><td colspan="18">暂无数据</td></tr>';
        }
        htm += "</tbody>";
        $(".js_tradeClassification .table").html(htm);
      },
      errCallback: function () {
        htm += '<tr><td colspan="18">暂无数据</td></tr>';
        htm += "</tbody>";
        $(".js_tradeClassification .table").html(htm);
      },
    });
    htm += "</tbody>";
    $(".js_tradeClassification .table").html(htm);
  },
};
if ($(".js_tradeClassification").length > 0) {
  tradeClassification.init();
}

/* 行业分类 end*/

/* 行业分类详情 */
var tradeDetail = {
  tradeUrl: sseQueryURL + "commonQuery.do?",
  tradeParam: {
    isPagination: false,
    csrcName: "",
    // 'csrcCode': "",
    REG_PROVINCE: "",
    sqlId: "COMMON_SSE_CP_GPJCTPZ_DQHYFL_XQ_L",
    CSRC_CODE: "", // CSRC门类行业代码
    COMPANY_STATUS: "",
  },
  tradeOption: [
    {
      name: "农业",
      value: "A01",
    },
    {
      name: "林业",
      value: "A02",
    },
    {
      name: "畜牧业",
      value: "A03",
    },
    {
      name: "渔业",
      value: "A04",
    },
    {
      name: "煤炭开采和洗选业",
      value: "B06",
    },
    {
      name: "石油和天然气开采业",
      value: "B07",
    },
    {
      name: "黑色金属矿采选业",
      value: "B08",
    },
    {
      name: "有色金属矿采选业",
      value: "B09",
    },
    {
      name: "非金属矿采选业",
      value: "B10",
    },
    {
      name: "开采辅助活动",
      value: "B11",
    },
    {
      name: "农副食品加工业",
      value: "C13",
    },
    {
      name: "食品制造业",
      value: "C14",
    },
    {
      name: "酒、饮料和精制茶制造业",
      value: "C15",
    },
    {
      name: "纺织业",
      value: "C17",
    },
    {
      name: "纺织服装、服饰业",
      value: "C18",
    },
    {
      name: "皮革、毛皮、羽毛及其制品和制鞋业",
      value: "C19",
    },
    {
      name: "木材加工和木、竹、藤、棕、草制品业",
      value: "C20",
    },
    {
      name: "家具制造业",
      value: "C21",
    },
    {
      name: "造纸和纸制品业",
      value: "C22",
    },
    {
      name: "印刷和记录媒介复制业",
      value: "C23",
    },
    {
      name: "文教、工美、体育和娱乐用品制造业",
      value: "C24",
    },
    {
      name: "石油加工、炼焦和核燃料加工业",
      value: "C25",
    },
    {
      name: "化学原料和化学制品制造业",
      value: "C26",
    },
    {
      name: "医药制造业",
      value: "C27",
    },
    {
      name: "化学纤维制造业",
      value: "C28",
    },
    {
      name: "橡胶和塑料制品业",
      value: "C29",
    },
    {
      name: "非金属矿物制品业",
      value: "C30",
    },
    {
      name: "黑色金属冶炼和压延加工业",
      value: "C31",
    },
    {
      name: "有色金属冶炼和压延加工业",
      value: "C32",
    },
    {
      name: "金属制品业",
      value: "C33",
    },
    {
      name: "通用设备制造业",
      value: "C34",
    },
    {
      name: "专用设备制造业",
      value: "C35",
    },
    {
      name: "汽车制造业",
      value: "C36",
    },
    {
      name: "铁路、船舶、航空航天和其他运输设备制造业",
      value: "C37",
    },
    {
      name: "电气机械和器材制造业",
      value: "C38",
    },
    {
      name: "计算机、通信和其他电子设备制造业",
      value: "C39",
    },
    {
      name: "仪器仪表制造业",
      value: "C40",
    },
    {
      name: "其他制造业",
      value: "C41",
    },
    {
      name: "废弃资源综合利用业",
      value: "C42",
    },
    {
      name: "电力、热力生产和供应业",
      value: "D44",
    },
    {
      name: "燃气生产和供应业",
      value: "D45",
    },
    {
      name: "水的生产和供应业",
      value: "D46",
    },
    {
      name: "房屋建筑业",
      value: "E47",
    },
    {
      name: "土木工程建筑业",
      value: "E48",
    },
    {
      name: "建筑安装业",
      value: "E49",
    },
    {
      name: "建筑装饰和其他建筑业",
      value: "E50",
    },
    {
      name: "批发业",
      value: "F51",
    },
    {
      name: "零售业",
      value: "F52",
    },
    {
      name: "铁路运输业",
      value: "G53",
    },
    {
      name: "道路运输业",
      value: "G54",
    },
    {
      name: "水上运输业",
      value: "G55",
    },
    {
      name: "航空运输业",
      value: "G56",
    },
    {
      name: "装卸搬运和运输代理业",
      value: "G58",
    },
    {
      name: "仓储业",
      value: "G59",
    },
    {
      name: "邮政业",
      value: "G60",
    },
    {
      name: "住宿业",
      value: "H61",
    },
    {
      name: "餐饮业",
      value: "H62",
    },
    {
      name: "电信、广播电视和卫星传输服务",
      value: "I63",
    },
    {
      name: "互联网和相关服务",
      value: "I64",
    },
    {
      name: "软件和信息技术服务业",
      value: "I65",
    },
    {
      name: "货币金融服务",
      value: "J66",
    },
    {
      name: "资本市场服务",
      value: "J67",
    },
    {
      name: "保险业",
      value: "J68",
    },
    {
      name: "其他金融业",
      value: "J69",
    },
    {
      name: "房地产业",
      value: "K70",
    },
    {
      name: "租赁业",
      value: "L71",
    },
    {
      name: "商务服务业",
      value: "L72",
    },
    {
      name: "研究和试验发展",
      value: "M73",
    },
    {
      name: "专业技术服务业",
      value: "M74",
    },
    {
      name: "生态保护和环境治理业",
      value: "N77",
    },
    {
      name: "公共设施管理业",
      value: "N78",
    },
    {
      name: "教育",
      value: "P82",
    },
    {
      name: "卫生",
      value: "Q83",
    },
    {
      name: "新闻和出版业",
      value: "R85",
    },
    {
      name: "广播、电视、电影和影视录音制作业",
      value: "R86",
    },
    {
      name: "文化艺术业",
      value: "R87",
    },
    {
      name: "体育",
      value: "R88",
    },
    {
      name: "综合",
      value: "S90",
    },
  ],
  toHref: "/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=", // 代码跳转地址
  init: function () {
    this.tradeParam.CSRC_CODE = getWindowHref().csrcCode
      ? decodeURI(getWindowHref().csrcCode)
      : "";
    this.tradeParam.csrcName = getWindowHref().csrcName
      ? decodeURI(getWindowHref().csrcName)
      : "";
    this.loadEvents();
    this.getStockList();
  },
  loadEvents: function () {
    var _this = this;
    // 地区下拉框数据渲染
    $(".js_trade .selectpicker").attr("title", _this.tradeParam.csrcName);
    $(".js_tradeDetail .title_lev2").html(
      _this.tradeParam.csrcName + "行业股票列表"
    );
    bootstrapSelect({
      method: function () {
        $(".js_trade .selectpicker").selectpicker({
          noneSelectedText: "请选择", // 默认显示内容
        });
        // 地区数据渲染
        $(".js_trade .selectpicker")
          .html(addSelectOption(_this.tradeOption))
          .selectpicker("refresh")
          .selectpicker("render");
        // 下拉框改变触发查询
        $(".js_trade .selectpicker").on("changed.bs.select", function (e) {
          _this.tradeParam.CSRC_CODE = e.target.value;
          _this.tradeParam.csrcName = _this.setCsrcName(e.target.value);
          _this.getStockList();
          $(".js_tradeDetail .title_lev2").html(
            _this.tradeParam.csrcName + "行业股票列表"
          );
        });
      },
    });
  },
  getStockList: function () {
    var _this = tradeDetail;
    if (_this.tradeParam.CSRC_CODE != "") {
      _this.tradeParam.COMPANY_STATUS = 2;
    }
    var htm =
      "<thead><tr><th>上市公司代码</th><th>上市公司名称</th><th>A股代码</th><th>B股代码</th></tr></thead><tbody>";
    getJSONP({
      type: "post",
      url: _this.tradeUrl,
      data: _this.tradeParam,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            htm += "<tr>";
            htm +=
              '<td class="text-nowrap"><a target="_blank" href="' +
              _this.toHref +
              v.COMPANY_CODE +
              '">' +
              v.COMPANY_CODE +
              "</a></td>";
            htm += '<td class="text-nowrap">' + v.FULL_NAME + "</td>";
            htm += '<td class="text-nowrap">' + v.A_STOCK_CODE + "</td>";
            htm += '<td class="text-nowrap">' + v.B_STOCK_CODE + "</td>";
            htm += "</tr>";
          });
          $(".js_tradeDetail .title_lev2").html(
            _this.tradeParam.csrcName + "行业股票列表"
          );
        } else {
          htm += '<tr><td colspan="18">暂无数据</td></tr>';
        }
        htm += "</tbody>";
        $(".js_tradeDetail .table").html(htm);
      },
      errCallback: function () {
        htm += '<tr><td colspan="18">暂无数据</td></tr>';
        htm += "</tbody>";
        $(".js_tradeDetail .table").html(htm);
      },
    });
  },
  setCsrcName: function (code) {
    var _this = this;
    var name = "";
    $.each(_this.tradeOption, function (k, v) {
      if (v.value == code) {
        name = v.name;
      }
    });
    return name;
  },
};
if ($(".js_tradeDetail").length > 0) {
  tradeDetail.init();
}

/* 行业分类详情 end*/

/**
 * 股票概况详情
 */
var company = {
  todayDate: get_systemDate_global(), // 当前系统时间
  lastTradeDate: get_lastTradeDate_global(), // 最近交易日
  lastYearDate: changeFormatDate(get_systemDate_global(), 0, 0, -3), // 3年前
  commonQueryUrl: sseQueryURL + "commonQuery.do?jsonCallBack=?",
  commonSoaQueryUrl: sseQueryURL + "commonSoaQuery.do?jsonCallBack=?",
  announcementUrl: sseQueryURL + "security/stock/queryCompanyBulletinNew.do", //上市公告接口
  typeNum: 0, // 成交统计查询类型
  indexSort: 0, // 成交统计查询ab股
  // 股票类型
  stockTypeParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_ZQLX_C",
    SEC_CODE: "",
  },
  stockCode: "", // 股票代码
  stockCodeA: "", // A股 用于成交统计
  stockCodeB: "", // B股 //用户成交统计
  isStar: false, // 是否科创板 flase 否 true是
  isAStock: true,  // 是否A股（主板A股、科创板A股）
  stockType: "", // 股票类型 A股 B股
  stockUnit: "万股", // 单位
  ashareUnit: "A股",
  bshareUnit: "B股",
  gfUnit: "股",
  marketIndexUrl: hq_queryUrl + "v1/sh1/snap/",
  marketIndexParams: {
    select: "",
  },
  // 成交统计
  isShareA: true,
  isShareB: true,
  isfrist: true, // 默认首次进入页面
  turnoverParams: {
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_CJGK_MRGK_C",
    SEC_CODE: "", // 股票代码
    TX_DATE: "", // 查询日期
    TX_DATE_MON: "", // 查询月份
    TX_DATE_YEAR: "", // 查询年份
  },
  dateTypeOption: [
    {
      name: "每日",
      value: "day",
    },
    {
      name: "月度",
      value: "month",
    },
    {
      name: "年度",
      value: "year",
    },
  ],
  // 融资融券
  marginUrl: sseQueryURL + "commonSoaQuery.do",
  marginParams: {
    isPagination: true,
    "pageHelp.pageSize": 5,
    "pageHelp.pageCount": 50,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 6,
    sqlId: "RZRQ_MX_INFO",
    beginDate: "",
    endDate: "",
    preStockCode: "",
  },
  // 大宗交易信息
  blockParams: {
    isPagination: true,
    "pageHelp.pageSize": 25,
    "pageHelp.pageNo:": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.endPage": 5,
    "pageHelp.cacheSize": 1,
    sqlId: "COMMON_SSE_XXPL_JYXXPL_DZJYXX_L_1",
    stockId: "",
    startDate: "",
    endDate: "",
  },
  // 交易公开信息
  mainPublicParams: {
    isPagination: true,
    "pageHelp.pageSize": 20,
    "pageHelp.pageCount": 5,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 5,
    token: "QUERY",
    sqlId: "JYGKXX_ZL",
    branchName: "",
    refType: "",
    secCode: "",
    tradeDateStart: "",
    tradeDateEnd: "",
  },
  starPublicParams: {
    isPagination: true,
    "pageHelp.pageSize": 20,
    "pageHelp.pageCount": 5,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 5,
    sqlId: "COMMON_SSE_PL_JYXXPL_KCBJYGKXX_GGCX_L",
    refType: "",
    secCode: "",
    beginDate: "",
    endDate: "",
  },
  // 公司公告
  announcementParams: {
    isPagination: true,
    "pageHelp.pageSize": 5,
    "pageHelp.cacheSize": 1,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.endPage": 1,
    START_DATE: "", //开始日期
    END_DATE: "", //结束日期
    SECURITY_CODE: "", //证券代码
    TITLE: "", //关键字
    BULLETIN_TYPE: "", //公告类型
  },
  // 监管-监管措施
  supervisioNmeasureParams: {
    isPagination: true,
    "pageHelp.pageSize": 15,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 5,
    extTeacher: "",
    extWTFL: "",
    siteId: "28",
    sqlId: "BS_KCB_GGLL_NEW",
    extGGLX: "",
    channelId: "10007,10008,10009,10010",
    extGGDL: "",
    order: "createTime|desc,stockcode|asc",
    type: "",
    stockcode: "",
    createTime: "",
    createTimeEnd: "",
  },
  // 监管-监管问询
  supervisionInquiryParams: {
    isPagination: true,
    "pageHelp.pageSize": 15,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 5,
    extTeacher: "",
    extWTFL: "",
    siteId: "28",
    sqlId: "BS_KCB_GGLL_NEW",
    extGGLX: "",
    channelId: "10743,10744,10012",
    extGGDL: "",
    order: "createTime|desc,stockcode|asc",
    type: "",
    stockcode: "",
    createTime: "",
    createTimeEnd: "",
  },
  // 监管-监管动态
  supervisionDynamicParams: {
    isPagination: true,
    "pageHelp.pageSize": 15,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 5,
    extTeacher: "",
    extWTFL: "",
    siteId: "28",
    sqlId: "BS_KCB_GGLL_NEW",
    extGGLX: "",
    channelId: "10005",
    extGGDL: "",
    order: "createTime|desc,stockcode|asc",
    type: "",
    stockcode: "",
    createTime: "",
    createTimeEnd: "",
  },
  // 公司概况参数
  companyOverviewParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_GPGK_GSGK_C",
    COMPANY_CODE: "",
  },
  // 公司概况数据
  companyOverviewData: {},
  // 上市日期参数
  listingDateParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_ZQPZ_GP_GPLB_SSR_C",
    productid: "",
  },
  // 上市日期数据
  listingDateData: [],
  // 董事会秘书英文名
nameOfBSParams: {
  isPagination: false,
  sqlId: "COMMON_SSE_ZQPZ_GP_GPLB_MSXX_C",
  productid: "",
},
  // 董事会秘书数据
  nameOfBSData: [],
  // 股本结构参数
  capitalstructureParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_GPGK_GBJG_C",
    COMPANY_CODE: "",
  },
  // 股本结构更新参数
  upcapitalstructureParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_CP_GPLB_GPGK_GBBDXQ_C",
    seq: "",
  },
  // 股本变动Url
  capitalchangeUrl:
    sseQueryURL + "security/stock/queryEquityChangeAndReason.do?jsonCallBack=?",
  // 股本变动参数
  capitalchangeParams: {
    isPagination: true,
    "pageHelp.pageSize": 5,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 1,
    companyCode: "",
  },
  // 十大股东排名接口
  topTenShareholdersParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_PL_XBRL_TOP10SHAREHOLDERS",
    stockId: "",
  },
  // 筹资情况-A首次/增发参数
  afFinancingParams: {
    isPagination: false,
    ISS_FLAG: "1,2",
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_CZQK_AGKCBZFSF_S",
    COMPANY_CODE: "",
    LIST_BOARD: "1",
    type: "inParams",
  },
  // 筹资情况-A配股参数
  arFinancingParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_CZQK_AGKCBPG_S",
    COMPANY_CODE: "",
    LIST_BOARD: "1",
  },
  // 筹资情况-B首次发行参数
  bfFinancingParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_ZQPZ_GPLB_CZQK_BGSCFX_S",
    productid: "",
  },
  // 筹资情况-B增发参数
  baFinancingParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_ZQPZ_GPLB_CZQK_BGZF_S",
    productid: "",
  },
  // 筹资情况-B配股参数
  brFinancingParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_ZQPZ_GPLB_CZQK_BGPG_S",
    productid: "",
  },
  // 筹资情况-科创板首发/增发参数
  sfFinancingParams: {
    isPagination: false,
    ISS_FLAG: "1,2",
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_CZQK_AGKCBZFSF_S",
    COMPANY_CODE: "",
    LIST_BOARD: "2",
    type: "inParams",
  },
  // 筹资情况-科创板配股参数
  srFinancingParams: {
    isPagination: false,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_CZQK_AGKCBPG_S",
    COMPANY_CODE: "",
    LIST_BOARD: "2",
  },
  // 筹资情况-首日表现
  fristDayParams: {
    isPagination: true,
    sqlId: "COMMON_SSE_SJ_GPSJ_SRBX_L",
    COMPANY_CODE: "",
    END_DATE: "",
    START_DATE: "",
  },
  // 利润分配-分红-A股
  abprofitParams: {
    isPagination: true,
    "pageHelp.pageSize": 5,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 1,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_FH_L",
    IS_STAR: "0",
    CONDITION_ZBA: "1",
    CONDITION_ZBB: "",
    COMPANY_CODE: "",
  },
  // 利润分配-分红-科创板
  sbprofitParams: {
    isPagination: true,
    "pageHelp.pageSize": 5,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 1,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_FH_L",
    IS_STAR: "1",
    CONDITION_ZBA: "1",
    CONDITION_ZBB: "",
    COMPANY_CODE: "",
  },
  // 利润分配-分红-B股
  bbprofitParams: {
    isPagination: true,
    "pageHelp.pageSize": 5,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 1,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_FH_L", // 新接口
    COMPANY_CODE: "", // 公司代码
    IS_STAR: "0", // 主板标识
    CONDITION_ZBB: "1", // 主板b查询条件
    CONDITION_ZBA: "",
  },
  // 利润分配-送股-A股
  agprofitParams: {
    isPagination: true,
    "pageHelp.pageSize": 5,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 1,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_SG_L", // 新接口
    COMPANY_CODE: "",
    IS_STAR: "0",
  },
  // 利润分配-送股-科创板
  sgprofitParams: {
    isPagination: true,
    "pageHelp.pageSize": 5,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 1,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_SG_L", // 新接口
    COMPANY_CODE: "",
    IS_STAR: "1",
  },
  // 利润分配-送股-B股
  bgprofitParams: {
    isPagination: true,
    "pageHelp.pageSize": 5,
    "pageHelp.pageNo": 1,
    "pageHelp.beginPage": 1,
    "pageHelp.cacheSize": 1,
    "pageHelp.endPage": 1,
    sqlId: "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_SG_L",
    COMPANY_CODE: "",
    IS_STAR: "0",
  },

  init: function () {
    this.loadEvents();
  },
  loadEvents: function () {
    var _this = this;
    var tabActive = getWindowHref().tabActive;
    if (tabActive) {
      $(".js_navTabs span").eq(tabActive).click();
    }
    // 隐藏默认的
    _this.initHide();

    // 隐藏代码查询框
    $(".productDetail_nav .sse_outerItem .sse_searchInput").hide();

    // 代码绑定查询
    $(".productDetail_nav .search_btn").on("click", function () {
      _this.setCompanyParams();
    });
    // 代码回车键触发查询
    $("#inputCode").on(bind_name, function (e) {
      var keyCode = e.keyCode || e.which || e.charCode;
      if (keyCode == 13) {
        _this.setCompanyParams();
      }
    });

    // 指标指数初始化 js_marketIndex
    // 一级标题无日期标签需拼接
    $(".js_marketIndex .title_lev1").append('<span class="new_date"></span>');

    // 成交统计初始化 js_turnover
    $(".js_turnover .title_lev1").append('<span class="new_date"></span>');
    $(".js_turnover .js_date input").attr("placeholder", "日期");
    laydate({
      elem: ".js_turnover .js_date input",
      theme: "#b50005",
      format: "yyyy-MM-dd",
      trigger: "click",
      btns: ["confirm"],
      min: "1990-12-19",
      max: _this.todayDate,
      done: function (value, date, endDate) {
        // 选择时间触发查询
        setTimeout(function () {
          _this.isDate = true;
          _this.setTurnoverParams();
        }, 500);
      },
    });
    // 融资融券初始化
    $(".js_margin .title_lev1").append('<span class="new_date"></span>');
    laydate({
      elem: ".js_margin .js_date input",
      theme: "#b50005",
      format: "yyyy-MM-dd",
      trigger: "click",
      btns: ["confirm"],
      min: "1990-12-19",
      done: function (value, date, endDate) {
        // 选择时间触发查询
        setTimeout(function () {
          _this.setMarginParams();
        }, 500);
      },
    });

    // 大宗交易统计初始化
    $(".js_block .title_lev1").append('<span class="new_date"></span>');
    laydate({
      elem: ".js_block .js_dateRange input",
      theme: "#b50005",
      format: "yyyy-MM-dd",
      range: true,
      trigger: "click",
      btns: ["clear", "confirm"],
      done: function (value, date, endDate) {
        // 选择时间触发查询
        setTimeout(function () {
          _this.setBlockParams();
        }, 500);
      },
    });

    // 信息交易公开
    laydate({
      elem: ".js_diclosurePublic .js_dateRange input",
      theme: "#b50005",
      format: "yyyy-MM-dd",
      range: true,
      trigger: "click",
      btns: ["confirm"],
      done: function (value, date, endDate) {
        // 选择时间触发查询
        setTimeout(function () {
          _this.setPublicParams();
        }, 500);
      },
    });

    // 相关公告
    // 公司公告初始化
    laydate({
      elem: ".js_announcement .js_dateRange input",
      theme: "#b50005",
      format: "yyyy-MM-dd",
      range: true,
      trigger: "click",
      btns: ["clear", "confirm"],
      value: "",
      done: function (value, date, endDate) {
        // 选择时间触发查询
        setTimeout(function () {
          _this.setAnnouncementParams();
        }, 500);
      },
    });
    _this.getAnnouncementType(); // 公告分类
    // 标题/关键字点击
    $(".js_announcement .js_keyWords .search_btn").on("click", function () {
      _this.setAnnouncementParams();
    });
    // 标题/关键字点击回车键触发查询
    $(".js_announcement .js_keyWords input").on(bind_name, function (e) {
      var keyCode = e.keyCode || e.which || e.charCode;
      if (keyCode == 13) {
        _this.setAnnouncementParams();
      }
    });

    // 监管初始化
    // 日期初始化
    laydate({
      elem: ".js_supervision .js_dateRange input",
      theme: "#b50005",
      format: "yyyy-MM-dd",
      range: true,
      min: "1990-12-19",
      max: _this.todayDate,
      trigger: "click",
      btns: ["clear", "confirm"],
      done: function (value, date, endDate) {
        // 选择时间触发查询
        setTimeout(function () {
          _this.setSupervisionParams();
        }, 500);
      },
    });

    // 公司信息

    // 股本结构 默认隐藏
    $(".js_capital .js_capitalstructure .title_lev2").find("a").remove(); // 移除默认A标签
    $(".js_capital .js_capitalchange .title_lev2").find("a").remove(); // 移除默认A标签

    // 利润分配 默认隐藏
    $(".js_profit .js_fhprofit .js_abprofit .title_lev3").find("a").remove(); // 移除默认A标签
    $(".js_profit .js_fhprofit .js_abprofit .title_lev3 .new_date").html(
      "* 单位:元人民币"
    ); // 添加默认单位
    $(".js_profit .js_fhprofit .js_bbprofit .title_lev3").find("a").remove(); // 移除默认A标签
    $(".js_profit .js_fhprofit .js_bbprofit .title_lev3 .new_date").html(
      "* 单位:元/美金"
    ); // 添加默认单位

    // 获取参数
    var code = getWindowHref().COMPANY_CODE;
    if (code) {
      _this.stockCode = code;
      _this.stockTypeParams.SEC_CODE = code;
      // 获取股票类型
      _this.getStockType1();
      // 获取公司概况
      _this.companyOverviewParams.COMPANY_CODE = _this.stockCode;
      _this.getCompanyOverview();
      // 获取行情走势
      require(["highstock"], function () {
        fnCodetype(code, function (type) {
          drawOneDayChart(code, "priceCharts", "", true, type);
        });
      });
      // 融资融券
      $(".js_margin").hide(); // 全部隐藏
      _this.marginParams.preStockCode = code;
      _this.getMarginDate();

      // 大宗交易信息
      _this.blockParams.stockId = code;
      _this.getBlockList(1);

      _this.announcementParams.SECURITY_CODE = code;
      _this.getAnnouncementList(1);

      // 监管措施
      _this.supervisioNmeasureParams.stockcode = code;
      _this.getSupervisioNmeasureList(1);
      // 监管问询
      _this.supervisionInquiryParams.stockcode = code;
      _this.getSupervisionInquiryList(1);
    }
  },
  // 代码/简称查询逻辑
  setCompanyParams: function () {
    var _this = company;
    var inputCode = $("#inputCode").val() ? $("#inputCode").val() : ""; // 证券代码
    if (!inputCode) {
      return false;
    }
    if (inputCode && (inputCode.length != 6 || isNaN(inputCode))) {
      bootstrapModal({
        text: "证券代码必须为6位数字",
      });
      return false;
    }
    popularize.saveCpyIntoCookie(
      $("#inputCode").val().trim(),
      $("#inputCode"),
      1
    );
    _this.initHide();

    _this.stockCode = inputCode;
    _this.stockTypeParams.SEC_CODE = inputCode;
    // 获取股票类型
    _this.getStockType1();

    // 获取行情走势
    require(["highstock"], function () {
      fnCodetype(inputCode, function (type) {
        drawOneDayChart(inputCode, "priceCharts", "", true, type);
      });
    });
    // 融资融券
    _this.marginParams.preStockCode = inputCode;
    _this.getMarginDate();

    // 大宗交易信息
    _this.blockParams.stockId = inputCode;
    _this.getBlockList(1);

    _this.announcementParams.START_DATE = _this.lastYearDate;
    _this.announcementParams.END_DATE = _this.todayDate;
    _this.announcementParams.SECURITY_CODE = inputCode;
    _this.getAnnouncementList(1);

    // 监管措施
    _this.supervisioNmeasureParams.stockcode = inputCode;
    _this.getSupervisioNmeasureList(1);
    // 监管问询
    _this.supervisionInquiryParams.stockcode = inputCode;
    _this.getSupervisionInquiryList(1);
  },
  // 默认隐藏的内容，初始和代码简称查询后调用
  initHide: function () {
    var _this = company;
    // 行情指标空加载
    var marketIndexNullHtml = _this.getMarketIndexNull();
    $(".js_marketIndex .table-responsive").html(marketIndexNullHtml);
    $(".js_marketIndex").hide();
    // 行情走势
    $("#priceCharts").html(
      '<div class="d-flex align-items-center justify-content-center noChart_message" style="height:300px;">暂无走势信息</div>'
    );
    // 融资融券隐藏
    $(".js_margin").hide(); // 全部隐藏
    // 大宗交易统计
    $(".js_block").hide(); // 全部隐藏
    // 交易信息公开
    $(".js_diclosurePublic").hide(); // 全部隐藏

    // 监管隐藏
    $(".js_supervision").hide(); // 全部隐藏
    $(".js_supervision .js_supervisioNmeasure").hide(); // 隐藏监管措施
    $(".js_supervision .js_supervisionInquiry").hide(); // 隐藏监管问询
    $(".js_supervision .js_supervisionDynamic").hide(); // 隐藏监管动态

    // 公司概况空初始加载
    _this.getCompanyOverviewNullHtml();

    // 股本结构 默认隐藏
    $(".js_capital").hide(); // 全部隐藏
    $(".js_capital .js_capitalstructure").hide(); // 隐藏股本结构
    $(".js_capital .js_capitalchange").hide(); // 隐藏股本变动及原因

    // 筹资情况 默认隐藏
    // $('.js_financing').hide();//全部隐藏
    $(".js_financing .js_affinancing").hide(); // 隐藏A首次发行
    $(".js_financing .js_aafinancing").hide(); // 隐藏A增发
    $(".js_financing .js_arfinancing").hide(); // 隐藏A配股
    $(".js_financing .js_bffinancing").hide(); // 隐藏B首次发行
    $(".js_financing .js_bafinancing").hide(); // 隐藏B增发
    $(".js_financing .js_brfinancing").hide(); // 隐藏B配股
    $(".js_financing .js_sffinancing").hide(); // 隐藏科创首次发行
    $(".js_financing .js_safinancing").hide(); // 隐藏科创增发
    $(".js_financing .js_srfinancing").hide(); // 隐藏科创配股

    // 利润分配 默认隐藏
    $(".js_profit .js_fhprofit .js_abprofit").hide(); // 隐藏分红A股
    $(".js_profit .js_fhprofit .js_bbprofit").hide(); // 隐藏分红B股
    $(".js_profit .js_sgprofit .js_agprofit").hide(); // 隐藏分红A股
    $(".js_profit .js_sgprofit .js_bgprofit").hide(); // 隐藏分红B股

    // 公司日历 默认隐藏
    $(".js_calendar").hide(); // 全部隐藏
  },
  // 获取行情指标
  getMarketIndex: function () {
    var _this = this,
      marketIndexHtml = "";
    $.ajax({
      type: "post",
      dataType: "jsonp",
      url: _this.marketIndexUrl + _this.stockCode,
      data: _this.marketIndexParams,
      jsonp: "callback",
      jsonpCallback:
        "jsonpCallback" + Math.floor(Math.random() * (100000000 + 1)),
      success: function (resulatData) {
        if (resulatData && resulatData.snap) {
          var data = resulatData.snap,
            dataDate = resulatData.date + "",
            dataTime = resulatData.time + "";
          dataDate = dataDate ? turnDateAddLine(dataDate) : "";
          dataTime = dataTime.length == 5 ? "0" + dataTime : dataTime;
          dataTime = dataTime
            ? dataTime.substring(0, 2) +
            ":" +
            dataTime.substring(2, 4) +
            ":" +
            dataTime.substring(4, 6)
            : "";
          $(".js_marketIndex .new_date").html(dataDate + " " + dataTime);
          var _last = data[1].toFixed(2), // 当前
            _lastColor = Comparative(data[1], data[7]), // 当前
            _chg_rate = data[2].toFixed(2), // 涨跌幅
            _chg_rateColor = Comparative(data[2], 0), // 涨跌幅
            _change = data[3].toFixed(2), // 涨跌
            _changeColor = Comparative(data[2], 0), // 涨跌
            _amount = (data[4] / 10000).toFixed(2), // 成交金额
            _volume = (data[5] / 100).toFixed(0), // 成交量
            _prev_close = data[7].toFixed(2), // 昨收
            _high = data[10].toFixed(2), // 最高
            _highColor = Comparative(data[10], data[7]), // 最高
            _low = data[11].toFixed(2), // 最低
            _lowColor = Comparative(data[11], data[7]), // 最低
            _turnover_ratio = data[13] / 100, // 换手率
            _amp_rate = data[15]; // 振幅
          var isHlt = false,
            _gdr_ratio,
            _gdr_prevpx,
            _gdr_currency;
          var _tradephase = data[12]; // 交易状态
          var isP1 = _tradephase.replace(/\s/gi, "") == "P1";
          if (_this.isAStock) {  // 是否是A股
            var _fp_volume = data[19] ? (data[19] / 100).toFixed(0) : 0; // 盘后交易量
            var _fp_amount = data[20] ? (data[20] / 10000).toFixed(2) : 0; // 盘后交易额
            var _fp_phaseArr = data[23] ? data[23].split("") : [];
            var _fp_phase = getFpphaseText(_fp_phaseArr);
          } else {
            // 沪伦通
            var _hlt_tag = data[19];
            if (_hlt_tag == "G") {
              isHlt = true;
              _gdr_ratio = data[20] ? data[20] : "-";
              _gdr_prevpx = data[21];
              _gdr_currency = data[22];
            }
          }
          marketIndexHtml += '<table class="table"><tr>';
          marketIndexHtml += '<td class="colData_title text-nowrap">最新</td>';
          marketIndexHtml +=
            '<td class="colData_title text-nowrap">涨跌幅</td>';
          marketIndexHtml += '<td class="colData_title text-nowrap">最高</td>';
          marketIndexHtml +=
            '<td class="colData_title text-nowrap">成交量(手)</td>';
          marketIndexHtml +=
            '<td class="colData_title text-nowrap">流通换手率</td>';
          if (isHlt)
            marketIndexHtml +=
              '<td class="colData_title text-nowrap">GDR转换比例</td>';
          if (_this.isAStock)
            marketIndexHtml +=
              '<td class="colData_title text-nowrap">盘后成交量(手)</td>';
          if (_this.isAStock)
            marketIndexHtml +=
              '<td class="colData_title text-nowrap">交易状态</td>';
          marketIndexHtml += "</tr><tr>";
          marketIndexHtml +=
            '<td class="colData_val text-nowrap ' +
            (_last != 0 && !isP1 ? _lastColor : "") +
            '">' +
            (_last != 0 && !isP1 ? _last : !isP1 ? "暂无成交" : "停牌") +
            "</td>"; // 最新
          marketIndexHtml +=
            '<td class="colData_val text-nowrap ' +
            (_last != 0 && !isP1 ? _chg_rateColor : "") +
            '">' +
            (_last != 0 && !isP1 ? _chg_rate + "%" : "--") +
            "</td>"; // 涨跌幅
          marketIndexHtml +=
            '<td class="colData_val text-nowrap ' +
            (_last != 0 && !isP1 ? _highColor : "") +
            '">' +
            (_last != 0 && !isP1 ? _high : "--") +
            "</td>"; // 最高
          marketIndexHtml +=
            '<td class="colData_val text-nowrap">' +
            (_last != 0 && !isP1 ? _volume : "--") +
            "</td>"; // 成交量
          marketIndexHtml +=
            '<td class="colData_val text-nowrap">' +
            (_last != 0 && !isP1 ? _turnover_ratio + "%" : "--") +
            "</td>"; // 换手率
          if (isHlt)
            marketIndexHtml +=
              '<td class="colData_val text-nowrap">' +
              (_last != 0 && !isP1 ? _gdr_ratio : "--") +
              "</td>"; // GDR转换比例
          if (_this.isAStock)
            marketIndexHtml +=
              '<td class="colData_val text-nowrap">' +
              (_last != 0 && !isP1 ? _fp_volume : "--") +
              "</td>"; // 盘后成交量
          if (_this.isAStock)
            marketIndexHtml +=
              '<td class="colData_val text-nowrap">' +
              getTradephaseText(_tradephase) +
              "</td>"; // 交易状态

          var colspan = 7;
          if (isHlt) {
            colspan = colspan + 1;
          }
          if (_this.isAStock) {
            colspan = colspan + 2;
          }
          marketIndexHtml += '<tr><td colspan="' + colspan + '"><hr></td></tr>';

          marketIndexHtml += "<tr>";
          marketIndexHtml += '<td class="colData_title text-nowrap">前收</td>';
          marketIndexHtml += '<td class="colData_title text-nowrap">涨跌</td>';
          marketIndexHtml += '<td class="colData_title text-nowrap">最低</td>';
          marketIndexHtml +=
            '<td class="colData_title text-nowrap">成交金额(万元)</td>';
          marketIndexHtml += '<td class="colData_title text-nowrap">振幅</td>';
          if (isHlt)
            marketIndexHtml +=
              '<td class="colData_title text-nowrap">GDR昨日收盘价</td>';
          if (_this.isAStock)
            marketIndexHtml +=
              '<td class="colData_title text-nowrap">盘后成交额(万元)</td>';
          if (_this.isAStock)
            marketIndexHtml +=
              '<td class="colData_title text-nowrap">盘后交易状态</td>';
          marketIndexHtml += "</tr><tr>";
          marketIndexHtml +=
            '<td class="colData_val text-nowrap">' +
            (!isP1 ? _prev_close : "--") +
            "</td>"; // 前收
          marketIndexHtml +=
            '<td class="colData_val text-nowrap ' +
            (_last != 0 && !isP1 ? _changeColor : "") +
            '">' +
            (_last != 0 && !isP1 ? _change : "--") +
            "</td>"; // 涨跌
          marketIndexHtml +=
            '<td class="colData_val text-nowrap ' +
            (_last != 0 && !isP1 ? _lowColor : "") +
            '">' +
            (_last != 0 && !isP1 ? _low : "--") +
            "</td>"; // 最低
          marketIndexHtml +=
            '<td class="colData_val text-nowrap">' +
            (_last != 0 && !isP1 ? _amount : "--") +
            "</td>"; // 成交金额
          marketIndexHtml +=
            '<td class="colData_val text-nowrap">' +
            (_last != 0 && !isP1 ? _amp_rate + "%" : "--") +
            "</td>"; // 振幅
          if (isHlt)
            marketIndexHtml +=
              '<td class="colData_val text-nowrap">' +
              (!isP1 ? _gdr_prevpx + _gdr_currency : "--") +
              "</td>"; // GDR昨日收盘价
          if (_this.isAStock)
            marketIndexHtml +=
              '<td class="colData_val text-nowrap">' +
              (_last != 0 && !isP1 ? _fp_amount : "--") +
              "</td>"; // 盘后成交额(万元)
          if (_this.isAStock)
            marketIndexHtml +=
              '<td class="colData_val text-nowrap">' + _fp_phase + "</td>"; // 盘后交易状态
          marketIndexHtml += "</tr></table>";
          $(".js_marketIndex .table-responsive").html(marketIndexHtml);
          if (resulatData.snap.length > 0) {
            $(".js_marketIndex").show();
          }
        } else {
          $(".js_marketIndex .new_date").html("");
          marketIndexHtml += _this.getMarketIndexNull();
          $(".js_marketIndex .table-responsive").html(marketIndexHtml);
        }
      },
      error: function () {
        $(".js_marketIndex .new_date").html("");
        marketIndexHtml += _this.getMarketIndexNull();
        $(".js_marketIndex .table-responsive").html(marketIndexHtml);
      },
    });
  },
  // 行情指标为空
  getMarketIndexNull: function () {
    var _this = this;
    var colspan = 7;
    if (_this.isAStock) {
      colspan = colspan + 2;
    }
    var marketIndexHtml = '<table class="table"><tr>';
    marketIndexHtml += '<td class="colData_title text-nowrap">最新</td>';
    marketIndexHtml += '<td class="colData_title text-nowrap">涨跌幅</td>';
    marketIndexHtml += '<td class="colData_title text-nowrap">最高</td>';
    marketIndexHtml += '<td class="colData_title text-nowrap">成交量(手)</td>';
    marketIndexHtml += '<td class="colData_title text-nowrap">流通换手率</td>';
    if (_this.isAStock)
      marketIndexHtml +=
        '<td class="colData_title text-nowrap">盘后成交量</td>';
    if (_this.isAStock)
      marketIndexHtml += '<td class="colData_title text-nowrap">交易状态</td>';
    marketIndexHtml += "</tr><tr>";
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += "</tr>";
    marketIndexHtml += '<tr><td colspan="' + colspan + '"><hr></td></tr>';
    marketIndexHtml += "<tr>";
    marketIndexHtml += '<td class="colData_title text-nowrap">前收</td>';
    marketIndexHtml += '<td class="colData_title text-nowrap">涨跌</td>';
    marketIndexHtml += '<td class="colData_title text-nowrap">最低</td>';
    marketIndexHtml +=
      '<td class="colData_title text-nowrap">成交金额(万元)</td>';
    marketIndexHtml += '<td class="colData_title text-nowrap">振幅</td>';
    if (_this.isAStock)
      marketIndexHtml +=
        '<td class="colData_title text-nowrap">盘后成交额(万元)</td>';
    if (_this.isAStock)
      marketIndexHtml +=
        '<td class="colData_title text-nowrap">盘后交易状态</td>';
    marketIndexHtml += "</tr><tr>";
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += '<td class="colData_val text-nowrap">-</td>';
    marketIndexHtml += "</tr></table>";
    return marketIndexHtml;
  },
  // 成交统计查询逻辑 isfrist true 第一次 isAgainValue 重新赋值
  initTurnover: function () {
    var _this = company,
      paramsHtml = '<div class="product_sel">';
    $(".js_turnover .product_sel").remove();
    // 种类查询条件处理
    if (
      (_this.stockCodeA && _this.stockCodeA != "-") ||
      (_this.stockCodeB && _this.stockCodeB != "-")
    ) {
      paramsHtml += '<div class="js_stockType">种类：';
      if (_this.stockCodeA && _this.stockCodeA != "-") {
        // A股存在 默认直接设置A股选中
        _this.isShareA = true;
        _this.turnoverParams.SEC_CODE = _this.stockCodeA;
        paramsHtml +=
          '<span class="active" data-value="1">' + _this.ashareUnit + "</span>";
      }
      if (
        (!_this.stockCodeA || _this.stockCodeA == "-") &&
        _this.stockCodeB &&
        _this.stockCodeB != "-"
      ) {
        // A股不存在，默认设置B股选中
        _this.isShareA = false;
        _this.turnoverParams.SEC_CODE = _this.stockCodeB;
        paramsHtml +=
          '<span class="active" data-value="2">' + _this.bshareUnit + "</span>";
      } else if (
        _this.stockCodeA &&
        _this.stockCodeA != "-" &&
        _this.stockCodeB &&
        _this.stockCodeB != "-"
      ) {
        // A股存在，B股不选中
        _this.isShareA = true;
        _this.turnoverParams.SEC_CODE = _this.stockCodeA;
        paramsHtml += '<span data-value="2">' + _this.bshareUnit + "</span>";
      }
      paramsHtml += "</div>";
    }
    paramsHtml += '<div class="js_dateType">周期：';
    paramsHtml += '<span data-value="day" class="active">每日</span>';
    paramsHtml += '<span data-value="month">月度</span>';
    paramsHtml += '<span data-value="year">年度</span>';
    paramsHtml += "</div>";
    paramsHtml += "</div>";
    $(".js_turnover .pro_inputBox").append(paramsHtml);
    $(".js_turnover .sse_colContent").on(
      "click",
      ".js_stockType span",
      function () {
        $(this).siblings("span").removeClass("active");
        $(this).addClass("active");
        var index1 = $(this).index();
        if (index1 == 1) {
          _this.isShareA = false;
        } else if (index1 == 0) {
          _this.isShareA = true;
        }
        _this.indexSort = index1;
        if (_this.indexSort == 0) {
          _this.turnoverParams.SEC_CODE = _this.stockCodeA;
        }
        if (_this.indexSort == 1) {
          _this.turnoverParams.SEC_CODE = _this.stockCodeB;
        }
        _this.isDate = false;
        _this.setTurnoverParams();
      }
    );
    $(".js_turnover .sse_colContent").on(
      "click",
      ".js_dateType span",
      function () {
        $(this).siblings("span").removeClass("active");
        $(this).addClass("active");
        var typeArr = ["date", "month", "year"];
        $(".js_turnover .js_date").html("");
        $(".js_turnover .js_date").append(
          '<div class="sse_searchInput"><input class="form-control sse_input" type="text" placeholder="日期" readonly><span class="bi-calendar4-week"></span></div>'
        );
        var index = $(this).index();
        _this.typeNum = index;
        var inputVal = _this.lastTradeDate;
        if (index == 1) {
          var temp = _this.lastTradeDate.substring(5, 7) - 1;
          if (temp < 1) {
            inputVal = _this.lastTradeDate.substring(0, 4) - 1 + "-12";
          } else if (temp < 10 && temp >= 1) {
            temp = "0" + temp;
            inputVal = _this.lastTradeDate.substring(0, 5) + temp;
          } else {
            inputVal = _this.lastTradeDate.substring(0, 5) + temp;
          }
        } else if (index == 2) {
          inputVal = _this.lastTradeDate.substring(0, 4) - 1;
        }
        $(".js_turnover .sse_searchInput .sse_input").val(inputVal);
        laydate({
          elem: ".js_turnover .js_date input",
          theme: "#b50005",
          format: "yyyy-MM-dd",
          min: "1990-12-19",
          max: _this.todayDate,
          isInitValue: true,
          type: typeArr[index],
          value: inputVal,
          trigger: "click",
          btns: ["confirm"],
          done: function (value, date, endDate) {
            // 选择时间触发查询
            setTimeout(function () {
              _this.isDate = true; //是否点击
              _this.setTransactionParanmByCDate();
            }, 500);
          },
        });
        _this.isDate = false;
        _this.setTransactionParanmByCDate(inputVal);
      }
    );
    _this.getTurnover();
  },
  // // 默认取最大日期数据点击事件
  defaultNull: function () {
    var _this = this;
    _this.turnoverParams.TX_DATE_MON = "";
    _this.turnoverParams.TX_DATE_YEAR = "";
    _this.turnoverParams.TX_DATE = "";
  },
  // 成交概况点击切换赋值参数
  setTransactionParanmByCDate: function (timeVal) {
    var _this = this;
    var timeStr = $(".js_turnover .sse_input").val();
    timeStr = timeStr ? timeStr : timeVal;
    _this.defaultNull();
    if (_this.typeNum == 0 || _this.typeNum == 3) {
      _this.turnoverParams.TX_DATE = _this.isDate == true ? timeStr : "";
      _this.turnoverParams["sqlId"] = "COMMON_SSE_CP_GPJCTPZ_GPLB_CJGK_MRGK_C";
    } else if (_this.typeNum == 1) {
      _this.turnoverParams.TX_DATE_MON =
        _this.isDate == true ? timeStr.replace(/\-/g, "") : "";
      _this.turnoverParams["sqlId"] = "COMMON_SSE_CP_GPJCTPZ_GPLB_CJGK_YDGK_C";
    } else if (_this.typeNum == 2) {
      _this.turnoverParams.TX_DATE_YEAR = _this.isDate == true ? timeStr : "";
      _this.turnoverParams["sqlId"] = "COMMON_SSE_CP_GPJCTPZ_GPLB_CJGK_NDGK_C";
    }
    _this.getTurnover();
  },
  // 成交统计查询逻辑 isfrist true 第一次 isAgainValue 重新赋值
  setTurnoverParams: function () {
    var _this = company;
    var searchDate = $(".js_turnover .js_date input").val()
      ? $(".js_turnover .js_date input").val()
      : "";
    // 日期类型
    var timevalue = $(".js_transactionOverview .js_dateType .active").attr(
      "data-value"
    );
    _this.defaultNull();
    if (timevalue == "year") {
      _this.turnoverParams.TX_DATE_YEAR =
        _this.isDate == true ? searchDate.substring(0, 4) : "";
    } else if (timevalue == "month") {
      _this.turnoverParams.TX_DATE_MON =
        _this.isDate == true ? searchDate.substring(0, 7) : "";
    } else {
      _this.turnoverParams.TX_DATE = _this.isDate == true ? searchDate : "";
    }
    _this.getTurnover();
  },
  // 获取成交统计
  getTurnover: function () {
    var _this = this,
      turnoverHtml = "";
    if (company.isfrist) {
      _this.defaultNull();
    }
    company.isfrist = false;
    var dateType = $(".js_turnover .js_dateType .active").attr("data-value")
      ? $(".js_turnover .js_dateType .active").attr("data-value")
      : "day";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.turnoverParams,
      successCallback: function (resultData) {
        this.currentDay = dateType;
        if (resultData && resultData.result && resultData.result.length > 0) {
          var data = resultData.result.filter(filtersArrDate, this)[0];
          if (data) {
            var _openPriceTitle,
              _openPrice,
              _closePriceTitle,
              _closePrice,
              _maxHighPriceTitle,
              _maxHighPrice,
              _minLowPriceTitle,
              _minLowPrice;
            if (_this.isShareA) {
              _openPriceTitle = "开盘价(元)";
              _openPrice = renum(data.OPEN_PRICE, 2);

              _closePriceTitle = "收盘价(元)";
              _closePrice = renum(data.CLOSE_PRICE, 2);

              _maxHighPriceTitle = "最高价(元)";
              _maxHighPrice = renum(data.HIGH_PRICE, 2);

              _minLowPriceTitle = "最低价(元)";
              _minLowPrice = renum(data.LOW_PRICE, 2);
            } else {
              _openPriceTitle = "开市价";
              _openPrice = renum(data.OPEN_PRICE, 3);

              _closePriceTitle = "收市价";
              _closePrice = renum(data.CLOSE_PRICE, 3);

              _maxHighPriceTitle = "最高成价(元)";
              _maxHighPrice = renum(data.HIGH_PRICE, 2);

              _minLowPriceTitle = "最低成价(元)";
              _minLowPrice = renum(data.LOW_PRICE, 2);
            }
            if (_this.typeNum == 0) {
              turnoverHtml += '<table class="table"><tr>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">市价总值(万元)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">流通市值(万元)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">成交量(' +
                _this.stockUnit +
                ")</td>";
              turnoverHtml +=
                '<td class="colData_title text-nowrap">成交金额(万元)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">' +
                _openPriceTitle +
                "</td>"; // 开市价或开盘价（元）
              turnoverHtml +=
                '<td class="colData_title text-nowrap">' +
                _closePriceTitle +
                "</td>"; // 收市价 或 收盘价（元）
              turnoverHtml += "</tr><tr>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                data.TOTAL_VALUE +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                data.NEGO_VALUE +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                renum(data.TRADE_VOL, 2) +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                data.TRADE_AMT +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' + _openPrice + "</td>"; // 开市价或开盘价（元）
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' + _closePrice + "</td>"; // 收市价 或 收盘价（元）
              turnoverHtml += "</tr>";

              turnoverHtml += '<tr><td colspan="6"><hr></td></tr>';

              turnoverHtml += "<tr>";
              turnoverHtml +=
                '<td class="colData_title text-nowrap">静态市盈率(倍)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">涨跌幅(%)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">总换手率(%)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">' +
                _maxHighPriceTitle +
                "</td>";
              turnoverHtml +=
                '<td class="colData_title text-nowrap">' +
                _minLowPriceTitle +
                "</td>";
              turnoverHtml += "</tr><tr>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                renum(data.PE_RATE, 2) +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                renum(data.CHANGE_RATE, 2) +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                renum(data.TO_RATE, 2) +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                _maxHighPrice +
                (dateType != "day"
                  ? "<br>(" +
                  (data.HIGH_PRICE_DATE
                    ? dateReplace(data.HIGH_PRICE_DATE)
                    : "-") +
                  ")"
                  : "") +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                _minLowPrice +
                (dateType != "day"
                  ? "<br>(" +
                  (data.LOW_PRICE_DATE
                    ? dateReplace(data.LOW_PRICE_DATE)
                    : "-") +
                  ")"
                  : "") +
                "</td>";
              turnoverHtml += "</tr>";
              turnoverHtml += "</table>";
              $(".js_turnover .table-responsive").html(turnoverHtml);
            } else {
              turnoverHtml += '<table class="table"><tr>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">市价总值(万元)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">流通市值(万元)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">成交量(' +
                _this.stockUnit +
                ")</td>";
              turnoverHtml +=
                '<td class="colData_title text-nowrap">成交金额(万元)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">' +
                _openPriceTitle +
                "</td>"; // 开市价或开盘价（元）
              turnoverHtml +=
                '<td class="colData_title text-nowrap">' +
                _closePriceTitle +
                "</td>"; // 收市价 或 收盘价（元）
              turnoverHtml += "</tr><tr>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                data.TOTAL_VALUE +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                data.NEGO_VALUE +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                renum(data.TRADE_VOL, 2) +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                data.TRADE_AMT +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' + _openPrice + "</td>"; // 开市价或开盘价（元）
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' + _closePrice + "</td>"; // 收市价 或 收盘价（元）
              turnoverHtml += "</tr>";

              turnoverHtml += '<tr><td colspan="6"><hr></td></tr>';

              turnoverHtml += "<tr>";
              turnoverHtml +=
                '<td class="colData_title text-nowrap">静态市盈率(倍)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">期间振幅(%)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">涨跌幅(%)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">总换手率(%)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">累计交易日</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">' +
                _maxHighPriceTitle +
                "</td>";

              turnoverHtml += "</tr><tr>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                renum(data.PE_RATE, 2) +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                renum(data.SWING_RATE, 2) +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                renum(data.CHANGE_RATE, 2) +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                renum(data.TO_RATE, 2) +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                (dateType == "day" ? "-" : data.ACCU_TRADE_DAYS) +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                _maxHighPrice +
                (dateType != "day"
                  ? "<br>(" +
                  (data.HIGH_PRICE_DATE
                    ? dateReplace(data.HIGH_PRICE_DATE)
                    : "-") +
                  ")"
                  : "") +
                "</td>";

              turnoverHtml += "</tr>";

              turnoverHtml += '<tr><td colspan="6"><hr></td></tr>';

              turnoverHtml += "<tr>";
              turnoverHtml +=
                '<td class="colData_title text-nowrap">' +
                _minLowPriceTitle +
                "</td>";
              turnoverHtml +=
                '<td class="colData_title text-nowrap">最高成交量(' +
                _this.stockUnit +
                ")</td>";
              turnoverHtml +=
                '<td class="colData_title text-nowrap">最低成交量(' +
                _this.stockUnit +
                ")</td>";

              turnoverHtml +=
                '<td class="colData_title text-nowrap">最高成交金额(万元)</td>';
              turnoverHtml +=
                '<td class="colData_title text-nowrap">最低成交金额(万元)</td>';
              turnoverHtml += "</tr><tr>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                _minLowPrice +
                (dateType != "day"
                  ? "<br>(" +
                  (data.LOW_PRICE_DATE
                    ? dateReplace(data.LOW_PRICE_DATE)
                    : "-") +
                  ")"
                  : "") +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                (dateType != "day"
                  ? data.HIGH_VOL +
                  "<br>(" +
                  (data.HIGH_VOL_DATE
                    ? dateReplace(data.HIGH_VOL_DATE)
                    : "-") +
                  ")"
                  : "-") +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                (dateType != "day"
                  ? data.LOW_VOL +
                  "<br>(" +
                  (data.LOW_VOL_DATE ? dateReplace(data.LOW_VOL_DATE) : "-") +
                  ")"
                  : "-") +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                (dateType != "day"
                  ? data.HIGH_AMT +
                  "<br>(" +
                  (data.HIGH_AMT_DATE
                    ? dateReplace(data.HIGH_AMT_DATE)
                    : "-") +
                  ")"
                  : "-") +
                "</td>";
              turnoverHtml +=
                '<td class="colData_val text-nowrap">' +
                (dateType != "day"
                  ? data.LOW_AMT +
                  "<br>(" +
                  (data.LOW_AMT_DATE ? dateReplace(data.LOW_AMT_DATE) : "-") +
                  ")"
                  : "-") +
                "</td>";
              turnoverHtml += "</tr></table>";
              $(".js_turnover .table-responsive").html(turnoverHtml);
            }
          } else {
            turnoverHtml += getTurnoverNull();
            $(".js_turnover .table-responsive").html(turnoverHtml);
          }
          // 日期类型
          var timevalue = $(".js_turnover .js_dateType .active").attr(
            "data-value"
          );
          // 传值日期控件
          var timeipt = $(".js_turnover .sse_input");
          if (resultData && resultData.result && resultData.result.length) {
            if (timevalue == "year") {
              timeipt.val(resultData.result[0].TX_DATE.substr(0, 4));
            } else if (timevalue == "month") {
              timeipt.val(
                resultData.result[0].TX_DATE.substr(0, 6).replace(
                  /^(\d{4})(\d{2})$/,
                  "$1-$2"
                )
              );
            } else if (timevalue == "day") {
              timeipt.val(
                resultData.result[0].TX_DATE.substr(0, 8).replace(
                  /^(\d{4})(\d{2})(\d{2})$/,
                  "$1-$2-$3"
                )
              );
            }
          }
        } else {
          $(".js_turnover .table-responsive").html(turnoverHtml);
        }
      },
      errCallback: function () {
        turnoverHtml += getTurnoverNull();
        $(".js_turnover .table-responsive").html(turnoverHtml);
      },
    });
  },
  // 获取成交统计空数据
  getTurnoverNull: function () {
    var _this = this;
    var turnoverHtml = '<table class="table"><tr>';
    turnoverHtml += '<td class="colData_title text-nowrap">市价总值(万元)</td>';
    turnoverHtml += '<td class="colData_title text-nowrap">流通市值(万元)</td>';
    turnoverHtml +=
      '<td class="colData_title text-nowrap">成交量(' +
      _this.stockUnit +
      ")</td>";
    turnoverHtml += '<td class="colData_title text-nowrap">成交金额(万元)</td>';
    turnoverHtml +=
      '<td class="colData_title text-nowrap">成交笔数(万笔)）</td>';
    turnoverHtml +=
      '<td class="colData_title text-nowrap">' +
      (_this.isShareA ? "开盘价(元)" : "开市价") +
      "</td>";
    turnoverHtml +=
      '<td class="colData_title text-nowrap">' +
      (_this.isShareA ? "收盘价(元)" : "收市价") +
      "</td>";
    turnoverHtml += "</tr><tr>";
    turnoverHtml += '<td class="colData_val text-nowrap">-</td>';
    turnoverHtml += '<td class="colData_val text-nowrap">-</td>';
    turnoverHtml += '<td class="colData_val text-nowrap">-</td>';
    turnoverHtml += '<td class="colData_val text-nowrap">-</td>';
    turnoverHtml += '<td class="colData_val text-nowrap">-</td>';
    turnoverHtml += '<td class="colData_val text-nowrap">-</td>';
    turnoverHtml += '<td class="colData_val text-nowrap">-</td>';
    turnoverHtml += "</tr>";

    turnoverHtml += '<tr><td colspan="6"><hr></td></tr>';

    turnoverHtml += "<tr>";
    turnoverHtml += '<td class="colData_title text-nowrap">静态市盈率(倍)</td>';
    turnoverHtml += '<td class="colData_title text-nowrap">期间振幅(%)</td>';
    turnoverHtml += '<td class="colData_title text-nowrap">涨跌幅(%)</td>';
    turnoverHtml += '<td class="colData_title text-nowrap">总换手率(%)</td>';
    turnoverHtml += "</tr><tr>";
    turnoverHtml += '<td class="colData_val text-nowrap">-</td>';
    turnoverHtml += '<td class="colData_val text-nowrap">-</td>';
    turnoverHtml += '<td class="colData_val text-nowrap">-</td>';
    turnoverHtml += '<td class="colData_val text-nowrap">-</td>';
    turnoverHtml += "</tr>";

    turnoverHtml += '<tr><td colspan="6"><hr></td></tr>';

    turnoverHtml += "<tr>";
    turnoverHtml += "</tr></table>";
    return turnoverHtml;
  },
  // 融资融券
  setMarginParams: function () {
    var _this = company;
    var searchDate = $(".js_margin .js_date input").val()
      ? $(".js_margin .js_date input").val()
      : "";
    $(".js_margin .title_lev1 .new_date").html(searchDate);
    _this.marginParams.beginDate = searchDate.replace(/\-/g, "");
    _this.marginParams.endDate = searchDate.replace(/\-/g, "");
    _this.getMarginList(1);
  },
  // 融资融券获取日期
  getMarginDate: function () {
    var _this = this;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.marginUrl,
      data: {
        isPagination: true,
        sqlId: "RZRQ_HZ_INFO",
        "pageHelp.pageSize": 100,
      },
      successCallback: function (data) {
        if (data && data.result && data.result[0] && data.result[0].opDate) {
          _this.marginParams.beginDate = data.result[0].opDate;
          _this.marginParams.endDate = data.result[0].opDate;
          $(".js_margin .title_lev1 .new_date").html(
            turnDateAddLine(data.result[0].opDate)
          );
          $(".js_margin .js_date input").val(
            turnDateAddLine(data.result[0].opDate)
          );
          _this.getMarginList(1);
        }
      },
      errCallback: function () {
        $(".js_margin").hide();
      },
    });
  },
  // 融资融券获取结果
  getMarginList: function (pageIndex) {
    var _this = this,
      emptyTr = '<tr><td colspan="8">暂无数据</td></tr>';
    if (!paginationChange(_this.marginParams, pageIndex, ".js_margin")) return; // 触发分页改变分页参数
    var marginHtml = "<thead><tr>";
    marginHtml += '<th class="text-nowrap">信用交易日期</th>';
    marginHtml += '<th class="text-right">融资余额(元)</th>';
    marginHtml += '<th class="text-right">融资买入额(元)</th>';
    marginHtml += '<th class="text-right">融资偿还额(元)</th>';
    marginHtml += '<th class="text-right">融券余量</th>';
    marginHtml += '<th class="text-right">融券卖出量</th>';
    marginHtml += '<th class="text-right">融券偿还量</th>';
    marginHtml += "</tr></thead><tbody>";
    _this.marginParams["pageHelp.pageSize"] = 5;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.marginUrl,
      data: _this.marginParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          if ($(".js_margin .title_lev1 .new_date").length > 0) {
            $(".js_margin .title_lev1 .new_date").html(
              data.result[0].tradedate
            );
          } else {
            $(".js_margin .title_lev1").append(
              '<span class="new_date">' + data.result[0].tradedate + "</span>"
            );
          }
          $.each(data.result, function (k, v) {
            marginHtml += "<tr>";
            marginHtml +=
              '<td class="text-nowrap align-middle">' +
              dateReplace(v.opDate) +
              "</td>"; // 信用交易日期
            marginHtml +=
              '<td class="text-right align-middle">' +
              formatNum(v.rzye) +
              "</td>"; // 融资余额
            marginHtml +=
              '<td class="text-right align-middle">' +
              formatNum(v.rzmre) +
              "</td>"; // 融资买入额
            marginHtml +=
              '<td class="text-right align-middle">' +
              formatNum(v.rzche) +
              "</td>"; // 融资偿还额
            marginHtml +=
              '<td class="text-right align-middle">' +
              formatNum(v.rqyl) +
              "</td>"; // 融券余量
            marginHtml +=
              '<td class="text-right align-middle">' +
              formatNum(v.rqmcl) +
              "</td>"; // 融券卖出量
            marginHtml +=
              '<td class="text-right align-middle">' +
              formatNum(v.rqchl) +
              "</td>"; // 融券偿还量
            marginHtml += "</tr>";
          });
          marginHtml += "</tbody>";
          $(".js_margin .table").html(marginHtml);
          Page.navigation(
            ".js_margin .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getMarginList"
          );
          $(".js_margin .pagination-box .change-pageSize").remove();
          $(".js_margin").show();
        } else {
          marginHtml += emptyTr;
          marginHtml += "</tbody>";
          $(".js_margin .table").html(marginHtml);
          $(".js_margin .pagination-box").html("");
        }
      },
      errCallback: function () {
        marginHtml += emptyTr;
        marginHtml += "</tbody>";
        $(".js_margin .table").html(marginHtml);
        $(".js_margin .pagination-box").html("");
      },
    });
  },
  // 大宗交易信息查询逻辑
  setBlockParams: function () {
    var _this = company;
    var date = $(".js_block .js_dateRange input").val()
      ? $(".js_block .js_dateRange input").val()
      : "";
    var beginDate = date ? date.substr(0, 10) : ""; // 开始日期
    var endDate = date ? date.substr(-10) : ""; // 结束日期
    if (beginDate && endDate && beginDate > endDate) {
      bootstrapModal({
        text: "开始日期不能大于结束日期",
      });
      return false;
    }
    _this.blockParams.stockId = _this.stockCode;
    _this.blockParams.startDate = beginDate;
    _this.blockParams.endDate = endDate;
    _this.getBlockList(1);
  },
  // 大宗交易信息查询
  getBlockList: function (pageIndex) {
    var _this = this,
      emptyTr = '<tr><td colspan="7">暂无数据</td></tr>';
    if (!paginationChange(_this.blockParams, pageIndex, ".js_block")) return; // 触发分页改变分页参数
    var blockHtml = "<thead><tr>";
    blockHtml += '<th class="text-nowrap">交易日期</th>';
    blockHtml += '<th class="text-right">成交价(元)</th>';
    blockHtml += '<th class="text-right">成交金额(万元)</th>';
    blockHtml += '<th class="text-right">成交量(万股/万份)</th>';
    blockHtml += "<th>买入营业部</th>";
    blockHtml += "<th>卖出营业部</th>";
    blockHtml += '<th class="text-nowrap">是否为专场</th>';
    blockHtml += "</tr></thead><tbody>";
    _this.blockParams["pageHelp.pageSize"] = 5;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.blockParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            blockHtml += "<tr>";
            blockHtml += '<td class="text-nowrap">' + v.tradedate + "</td>"; // 交易日期
            blockHtml += '<td class="text-right">' + v.tradeprice + "</td>"; // 成交价(元)
            blockHtml += '<td class="text-right">' + v.tradeamount + "</td>"; // 成交金额(万元)
            blockHtml += '<td class="text-right">' + v.tradeqty + "</td>"; // 成交量(万股/万份)
            blockHtml +=
              '<td><span class="table_titlewrap">' +
              v.branchbuy +
              "</span></td>"; // 买入营业部
            blockHtml +=
              '<td><span class="table_titlewrap">' +
              v.branchsell +
              "</span></td>"; // 卖出营业部
            blockHtml += '<td class="text-nowrap">' + v.ifZc + "</td>"; // 是否为专场
            blockHtml += "</tr>";
          });
          blockHtml += "</tbody>";
          $(".js_block .table").html(blockHtml);
          Page.navigation(
            ".js_block .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getBlockList"
          );
          $(".js_block .pagination-box .change-pageSize").remove();
          $(".js_block").show();
        } else {
          blockHtml += emptyTr;
          blockHtml += "</tbody>";
          $(".js_block .table").html(blockHtml);
          $(".js_block .pagination-box").html("");
        }
      },
      errCallback: function () {
        blockHtml += emptyTr;
        blockHtml += "</tbody>";
        $(".js_block .table").html(blockHtml);
        $(".js_block .pagination-box").html("");
      },
    });
  },
  // 交易信息公开参数处理 mainPublicParams starPublicParams
  setPublicParams: function () {
    var _this = company;
    var searchDate = $(".js_diclosurePublic .js_dateRange input").val()
      ? $(".js_diclosurePublic .js_dateRange input").val()
      : "",
      beginDate = searchDate ? searchDate.substr(0, 10) : "",
      endDate = searchDate ? searchDate.substr(-10) : "";
    if (beginDate && endDate && beginDate > endDate) {
      bootstrapModal({
        text: "开始日期不能大于结束日期",
      });
      return false;
    }
    if (!_this.isStar) {
      // 主板
      _this.mainPublicParams.tradeDateStart = beginDate;
      _this.mainPublicParams.tradeDateEnd = endDate;
    } else {
      // 科创板
      _this.starPublicParams.beginDate = beginDate
        ? beginDate.replace(/-/g, "")
        : "";
      _this.starPublicParams.endDate = endDate ? endDate.replace(/-/g, "") : "";
    }
    _this.getDiclosurePublicList(1);
  },
  // 交易信息公开查询 mainPublicParams starPublicParams
  getDiclosurePublicList: function (pageIndex) {
    var _this = this,
      emptyTr = '<tr><td colspan="3">暂无数据</td></tr>',
      publicUrl,
      publicParams;
    if (!_this.isStar) {
      // 主板
      publicUrl = _this.commonSoaQueryUrl;
      publicParams = _this.mainPublicParams;
    } else {
      // 科创板
      publicUrl = _this.commonQueryUrl;
      publicParams = _this.starPublicParams;
    }
    if (!paginationChange(publicParams, pageIndex, ".js_diclosurePublic"))
      return; // 触发分页改变分页参数
    var publicHtml = "<thead><tr>";
    publicHtml += '<th class="text-nowrap">披露时间</th>';
    publicHtml += '<th class="text-nowrap">披露原因</th>';
    publicHtml += '<th class="text-center">详情</th>';
    publicHtml += "</tr></thead><tbody>";
    publicParams["pageHelp.pageSize"] = 5;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: publicUrl,
      data: publicParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            var disclosureDate, disclosureType, hrefUrl;
            if (!_this.isStar) {
              // 主板
              disclosureDate = v.abnormalEnd.datatimexia();
              
              var _name = dataPackage("main", v.refType, "").names;
              for (var i = 0; i < _name.length; i++) {
                if (v.tradeDate >= _name[i][0] && (i + 1 >= _name.length || v.tradeDate < _name[i + 1][0])) {
                  disclosureType = _name[i][1];
                  break;
                }
              }

              hrefUrl =
                "/disclosure/diclosure/public/dailydata/detail.shtml?secCode=" +
                v.secCode +
                "&secAbbr=" +
                v.secAbbr +
                "&refType=" +
                v.refType +
                "&tradeDate=" +
                v.tradeDate;
            } else {
              disclosureDate = turnDateAddLine(v.TRADE_DATE);
              
              var _name = dataPackage("star", v.TYPE, "").names;
              for (var i = 0; i < _name.length; i++) {
                if (v.TRADE_DATE >= _name[i][0] && (i + 1 >= _name.length || v.TRADE_DATE < _name[i + 1][0])) {
                  disclosureType = _name[i][1];
                  break;
                }
              }

              hrefUrl =
                "/disclosure/diclosure/public/dailydatatib/detail.shtml?secCode=" +
                v.SEC_CODE +
                "&secAbbr=" +
                v.SEC_ABBR +
                "&refType=" +
                v.TYPE +
                "&tradeDate=" +
                v.TRADE_DATE;
            }
            publicHtml += "<tr>";
            publicHtml +=
              '<td class="text-nowrap align-middle">' +
              disclosureDate +
              "</td>"; // 披露时间
            publicHtml +=
              '<td class="text-nowrap align-middle">' +
              disclosureType +
              "</td>"; // 披露原因
            if ((v.refType == "52" && !_this.isStar) || (v.TYPE == "15" && _this.isStar)) {
              publicHtml +=
                '<td class="text-center"></td>';
            } else {
              publicHtml +=
                '<td class="text-center"><a href="' +
                hrefUrl +
                '" target="_blank"><i class="iconfont iconwenben"></i></a></td>'; // 详情
            }
            publicHtml += "</tr>";
          });
          publicHtml += "</tbody>";
          $(".js_diclosurePublic .table").html(publicHtml);
          Page.navigation(
            ".js_diclosurePublic .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getDiclosurePublicList"
          );
          $(".js_diclosurePublic .pagination-box .change-pageSize").remove();
          $(".js_diclosurePublic").show();
        } else {
          publicHtml += emptyTr;
          publicHtml += "</tbody>";
          $(".js_diclosurePublic .table").html(publicHtml);
          $(".js_diclosurePublic .pagination-box").html("");
        }
      },
      errCallback: function () {
        publicHtml += emptyTr;
        publicHtml += "</tbody>";
        $(".js_diclosurePublic .table").html(publicHtml);
        $(".js_diclosurePublic .pagination-box").html("");
      },
    });
  },
  // 公司公告查询逻辑
  setAnnouncementParams: function () {
    var _this = company;
    var date = $(".js_announcement .js_dateRange input").val()
      ? $(".js_announcement .js_dateRange input").val()
      : "";
    var beginDate = date ? date.substr(0, 10) : ""; // 开始日期
    var endDate = date ? date.substr(-10) : ""; // 结束日期
    if (beginDate && endDate && beginDate > endDate) {
      bootstrapModal({
        text: "开始日期不能大于结束日期",
      });
      return false;
    }
    var startYear = changeFormatDate(beginDate, 0, 0, 3);
    if (new Date(startYear) - new Date(endDate) < 0) {
      bootstrapModal({
        text: "查询只提供日期间隔不超过3年的公告",
      });
      return false;
    }
    var BULLETIN_TYPE = $(".js_announcement .js_type .selectpicker").val()
      ? $(".js_announcement .js_type .selectpicker").val().join(",")
      : "";
    _this.announcementParams.START_DATE = beginDate;
    _this.announcementParams.END_DATE = endDate;
    _this.announcementParams.SECURITY_CODE = _this.stockCode;
    _this.announcementParams.BULLETIN_TYPE = BULLETIN_TYPE;
    _this.announcementParams.TITLE = $(
      ".js_announcement .js_keyWords input"
    ).val()
      ? $.trim($(".js_announcement .js_keyWords input").val())
      : "";
    _this.getAnnouncementList(1);
  },
  // 获取公司公告类型
  getAnnouncementType: function () {
    var _this = company;
    $.getJSON(
      "/xhtml/disclosure/listedinfo/announcement/json/announce_type.json?v=" +
      Math.random(),
      function (data) {
        if (data && data.publishData && data.publishData.length > 0) {
          var typeOption = "",
            index = 0;
          $.each(data.publishData, function (k, v) {
            var code = v.announceTypeCode;
            if (v.childData && v.childData.length > 0) {
              var childCode = [code];
              $.each(v.childData, function (childk, childv) {
                childCode.push(childv.announceTypeCode);
              });
              code = childCode.join(",");
            }
            typeOption +=
              '<option value="' +
              code +
              '">' +
              v.announceTypeId +
              "." +
              v.announceTypeName +
              "</option>";
          });
          $(".js_announcement .js_type .selectpicker")
            .attr("multiple", "multiple")
            .attr("data-live-search", "true")
            .attr("data-live-search-placeholder", "分类编号/分类名称")
            .attr("data-none-results-text", "");
          bootstrapSelect({
            method: function () {
              // 类型渲染下拉框
              $(".js_announcement .js_type .selectpicker")
                .html(typeOption)
                .selectpicker({
                  noneSelectedText: "请选择分类",
                })
                .selectpicker("refresh")
                .selectpicker("render");
              // 类型改变触发查询
              $(".js_announcement .js_type .selectpicker").on(
                "changed.bs.select",
                function (e) {
                  _this.setAnnouncementParams();
                }
              );
            },
          });
        }
      }
    );
  },
  // 获取公司公告
  getAnnouncementList: function (pageIndex) {
    var _this = this,
      emptyTr = '<tr><td colspan="3">暂无数据</td></tr>';
    if (
      !paginationChange(_this.announcementParams, pageIndex, ".js_announcement")
    )
      return; // 触发分页改变分页参数
    var announcementHtml = "<thead><tr>";
    announcementHtml += "<th>公告标题</th>";
    announcementHtml += '<th class="text-nowrap">公告分类</th>';
    announcementHtml += '<th class="text-nowrap">公告时间</th>';
    announcementHtml += "</tr></thead><tbody>";
    _this.announcementParams["pageHelp.pageSize"] = 10;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.announcementUrl,
      data: _this.announcementParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          for (var i = 0; i < data.result.length; i++) {
            var j = data.result[i].length; // 本条数量
            $.each(data.result[i], function (k, v) {
              announcementHtml += "<tr>";
              announcementHtml +=
                '<td><a class="table_titlewrap" href="' +
                staticBulletinUrl +
                (v.URL ? v.URL : "") +
                '" target="_blank">' +
                (v.IS_HOLDER_DISCLOSE == 1 ? "<em>股东自行披露</em>" : "") +
                (v.TITLE ? v.TITLE : "") +
                "</a></td>";
              announcementHtml +=
                '<td class="text-nowrap">' +
                (v.BULLETIN_TYPE_DESC ? v.BULLETIN_TYPE_DESC : "") +
                "</td>";
              announcementHtml +=
                '<td class="text-nowrap codeNameWidth width_bag left_border' +
                (k < j - 1 ? " transparent_bottom" : "") +
                (k > 0 ? " transparent_top" : "") +
                (j > 1 && k != 0 ? " transparent_top" : "") +
                '">' +
                (k == 0 ? (v.SSEDATE ? v.SSEDATE : "-") : "") +
                "</td>";
              announcementHtml += "</tr>";
            });
          }

          announcementHtml += "</tbody>";
          $(".js_announcement .table").html(announcementHtml);
          Page.navigation(
            ".js_announcement .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getAnnouncementList"
          );
          $(".js_announcement .pagination-box .change-pageSize").remove();
          $(".js_announcement").show();
        } else {
          announcementHtml += emptyTr;
          announcementHtml += "</tbody>";
          $(".js_announcement .table").html(announcementHtml);
          $(".js_announcement .pagination-box").html("");
        }
      },
      errCallback: function () {
        announcementHtml += emptyTr;
        announcementHtml += "</tbody>";
        $(".js_announcement .table").html(announcementHtml);
        $(".js_announcement .pagination-box").html("");
      },
    });
  },
  // 监管查询逻辑
  setSupervisionParams: function () {
    var _this = company;
    var date = $(".js_supervision .js_dateRange input").val()
      ? $(".js_supervision .js_dateRange input").val()
      : "";
    var beginDate = date ? date.substr(0, 10) : ""; // 开始日期
    var endDate = date ? date.substr(-10) : ""; // 结束日期
    if (beginDate && endDate && beginDate > endDate) {
      bootstrapModal({
        text: "开始日期不能大于结束日期",
      });
      return false;
    }
    _this.supervisioNmeasureParams.stockcode = _this.stockCode;
    _this.supervisioNmeasureParams.createTime = beginDate
      ? beginDate + " 00:00:00"
      : "";
    _this.supervisioNmeasureParams.createTimeEnd = endDate
      ? endDate + " 23:59:59"
      : "";
    _this.getSupervisioNmeasureList(1);
    // 监管问询
    _this.supervisionInquiryParams.stockcode = _this.stockCode;
    _this.supervisionInquiryParams.createTime = beginDate
      ? beginDate + " 00:00:00"
      : "";
    _this.supervisionInquiryParams.createTimeEnd = endDate
      ? endDate + " 23:59:59"
      : "";
    _this.getSupervisionInquiryList(1);
    // 监管动态 需求不定，暂时舍弃
    _this.supervisionDynamicParams.stockcode = _this.stockCode;
    _this.supervisionDynamicParams.createTime = beginDate;
    _this.supervisionDynamicParams.createTimeEnd = endDate;
  },
  // 监管-监管措施
  getSupervisioNmeasureList: function (pageIndex) {
    var _this = this,
      emptyTr = '<tr><td colspan="5">暂无数据</td></tr>';
    if (
      !paginationChange(
        _this.supervisioNmeasureParams,
        pageIndex,
        ".js_supervisioNmeasure"
      )
    )
      return; // 触发分页改变分页参数
    var supervisioNmeasureHtml = "<thead><tr>";
    supervisioNmeasureHtml += '<th class="text-nowrap">监管类型</th>';
    supervisioNmeasureHtml += "<th>处理事由</th>";
    supervisioNmeasureHtml += "<th>涉及对象</th>";
    supervisioNmeasureHtml += '<th class="text-nowrap">处理日期</th>';
    supervisioNmeasureHtml += "</tr></thead><tbody>";
    _this.supervisioNmeasureParams["pageHelp.pageSize"] = 5;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonSoaQueryUrl,
      data: _this.supervisioNmeasureParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            var extWTFL = v.extWTFL ? $.trim(v.extWTFL).replace(/,/g, "") : "";
            supervisioNmeasureHtml += "<tr>";
            supervisioNmeasureHtml += '<td class="text-nowrap">'; // 监管类型
            supervisioNmeasureHtml +=
              extWTFL && extWTFL == "监管关注" ? "书面警示" : extWTFL;
            supervisioNmeasureHtml += "</td>";
            supervisioNmeasureHtml += "<td>"; // 处理事由
            if (
              !v.docURL ||
              (v.channelId == "10010" &&
                (v.extTYPE == "监管工作函" || v.extTYPE == "规范运作建议书"))
            ) {
              supervisioNmeasureHtml += v.docTitle;
            } else {
              supervisioNmeasureHtml +=
                '<a href="http://' +
                v.docURL +
                '" target="_blank">' +
                v.docTitle +
                "</a>";
            }
            supervisioNmeasureHtml += "</td>";
            supervisioNmeasureHtml +=
              '<td class="text-nowrap">' +
              (v.extTeacher ? v.extTeacher : "-") +
              "</td>"; // 涉及对象
            supervisioNmeasureHtml +=
              '<td class="text-nowrap">' +
              v.createTime.substring(0, 10) +
              "</td>"; // 处理日期
            supervisioNmeasureHtml += "</tr>";
          });
          supervisioNmeasureHtml += "</tbody>";
          $(".js_supervisioNmeasure .table").html(supervisioNmeasureHtml);
          Page.navigation(
            ".js_supervisioNmeasure .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getSupervisioNmeasureList"
          );
          $(".js_supervisioNmeasure .pagination-box .change-pageSize").remove();
          $(".js_supervision").show().find(".js_supervisioNmeasure").show();
        } else {
          supervisioNmeasureHtml += emptyTr;
          supervisioNmeasureHtml += "</tbody>";
          $(".js_supervisioNmeasure .table").html(supervisioNmeasureHtml);
          $(".js_supervisioNmeasure .pagination-box").html("");
        }
      },
      errCallback: function () {
        supervisioNmeasureHtml += emptyTr;
        supervisioNmeasureHtml += "</tbody>";
        $(".js_supervisioNmeasure .table").html(supervisioNmeasureHtml);
        $(".js_supervisioNmeasure .pagination-box").html("");
      },
    });
  },
  // 监管-监管问询
  getSupervisionInquiryList: function (pageIndex) {
    var _this = this,
      emptyTr = '<tr><td colspan="3">暂无数据</td></tr>';
    if (
      !paginationChange(
        _this.supervisionInquiryParams,
        pageIndex,
        ".js_supervisionInquiry"
      )
    )
      return; // 触发分页改变分页参数
    var supervisionInquiryHtml = "<thead><tr>";
    supervisionInquiryHtml += '<th class="text-nowrap">发函日期</th>';
    supervisionInquiryHtml += '<th class="text-nowrap">监管问询类型</th>';
    supervisionInquiryHtml += "<th>标题</th>";
    supervisionInquiryHtml += "</tr></thead><tbody>";
    _this.supervisionInquiryParams["pageHelp.pageSize"] = 5;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonSoaQueryUrl,
      data: _this.supervisionInquiryParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            var extWTFL = v.extWTFL ? $.trim(v.extWTFL).replace(/,/g, "") : "";
            supervisionInquiryHtml += "<tr>";
            supervisionInquiryHtml +=
              '<td class="text-nowrap">' +
              v.createTime.substring(0, 10) +
              "</td>"; // 发函日期
            supervisionInquiryHtml +=
              '<td class="text-nowrap">' + extWTFL + "</td>";
            supervisionInquiryHtml += '<td><span class="table_titlewrap">'; // 标题
            if (!v.docURL) {
              supervisionInquiryHtml += v.docTitle;
            } else {
              supervisionInquiryHtml +=
                '<a href="http://' +
                v.docURL +
                '" target="_blank">' +
                v.docTitle +
                "</a>";
            }
            supervisionInquiryHtml += "</span></td>";
            supervisionInquiryHtml += "</tr>";
          });
          supervisionInquiryHtml += "</tbody>";
          $(".js_supervisionInquiry .table").html(supervisionInquiryHtml);
          Page.navigation(
            ".js_supervisionInquiry .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getSupervisionInquiryList"
          );
          $(".js_supervisionInquiry .pagination-box .change-pageSize").remove();
          $(".js_supervision").show().find(".js_supervisionInquiry").show();
        } else {
          supervisionInquiryHtml += emptyTr;
          supervisionInquiryHtml += "</tbody>";
          $(".js_supervisionInquiry .table").html(supervisionInquiryHtml);
          $(".js_supervisionInquiry .pagination-box").html("");
        }
      },
      errCallback: function () {
        supervisionInquiryHtml += emptyTr;
        supervisionInquiryHtml += "</tbody>";
        $(".js_supervisionInquiry .table").html(supervisionInquiryHtml);
        $(".js_supervisionInquiry .pagination-box").html("");
      },
    });
  },
  // 监管-监管动态
  getSupervisionDynamicList: function (pageIndex) {
    var _this = this,
      emptyTr = '<tr><td colspan="2">暂无数据</td></tr>';
    if (
      !paginationChange(
        _this.supervisionDynamicParams,
        pageIndex,
        ".js_supervisionDynamic"
      )
    )
      return; // 触发分页改变分页参数
    var supervisionDynamicHtml = "<thead><tr>";
    supervisionDynamicHtml += "<th>标题</th>";
    supervisionDynamicHtml += '<th class="text-nowrap">日期</th>';
    supervisionDynamicHtml += "</tr></thead><tbody>";
    _this.supervisionDynamicParams["pageHelp.pageSize"] = 5;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonSoaQueryUrl,
      data: _this.supervisionDynamicParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            var extWTFL = v.extWTFL ? $.trim(v.extWTFL).replace(/,/g, "") : "";
            supervisionDynamicHtml += "<tr>";
            supervisionDynamicHtml += "<td>"; // 标题
            if (!v.docURL) {
              supervisionDynamicHtml += v.docTitle;
            } else {
              supervisionDynamicHtml +=
                '<a href="http://' +
                v.docURL +
                '" target="_blank">' +
                v.docTitle +
                "</a>";
            }
            supervisionDynamicHtml += "</td>";
            supervisionDynamicHtml +=
              '<td class="text-nowrap">' +
              v.createTime.substring(0, 10) +
              "</td>"; // 发函日期
            supervisionDynamicHtml += "</tr>";
          });
          supervisionDynamicHtml += "</tbody>";
          $(".js_supervisionDynamic .table").html(supervisionDynamicHtml);
          Page.navigation(
            ".js_supervisionDynamic .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getSupervisionDynamicList"
          );
          $(".js_supervisionDynamic .pagination-box .change-pageSize").remove();
          $(".js_supervision").show().find(".js_supervisionDynamic").show();
        } else {
          supervisionDynamicHtml += emptyTr;
          supervisionDynamicHtml += "</tbody>";
          $(".js_supervisionDynamic .table").html(supervisionDynamicHtml);
          $(".js_supervisionDynamic .pagination-box").html("");
        }
      },
      errCallback: function () {
        supervisionDynamicHtml += emptyTr;
        supervisionDynamicHtml += "</tbody>";
        $(".js_supervisionDynamic .table").html(supervisionDynamicHtml);
        $(".js_supervisionDynamic .pagination-box").html("");
      },
    });
  },
  // 获取股本结构
  getCapitalstructure: function () {
    var _this = this,
      apitalstructureHtml = "";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.capitalstructureParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0 && data.result[0]) {
          var resultData = data.result[0];
          // 赋值-数据更新日期
          $(".js_capitalstructure .new_date").html(
            "数据日期：" + dateFormat(resultData.TRADE_DATE)
          );
          $(".js_capitalchange .new_date").html(
            "数据日期：" + resultData.TRADE_DATE
          );

          // 追加单位
          if ($(".js_capitalstructure .table_tip").length == 0)
            $(".js_capitalstructure .title_lev2").append(
              '<div style="display:inline-block;float:right;line-height:30px;" class="table_tip">单位：' +
              _this.stockUnit +
              "</div>"
            );

          apitalstructureHtml += "<tbody>";
          apitalstructureHtml +=
            '<tr><td colspan="2">境内上市股票合计</td><td colspan="2" class="text-right">' +
            (resultData.TOTAL_DOMESTIC_VOL
              ? resultData.TOTAL_DOMESTIC_VOL
              : "-") +
            "</td></tr>";
          apitalstructureHtml +=
            '<tr><td>有限售流通股</td><td class="text-right table_borderRight">' +
            (resultData.A_LIMIT_VOL ? resultData.A_LIMIT_VOL : "-") +
            "</td>";
          apitalstructureHtml +=
            '<td>无限售流通股</td><td class="text-right">' +
            (resultData.TOTAL_UNLIMIT_VOL
              ? resultData.TOTAL_UNLIMIT_VOL
              : "-") +
            "</td></tr>";
          apitalstructureHtml +=
            '<tr><td>其中：特别表决权股</td><td class="text-right table_borderRight">' +
            (resultData.SPECIAL_VOL ? resultData.SPECIAL_VOL : "-") +
            "</td>";
          apitalstructureHtml +=
            '<td>无限售流通A股/CDR</td><td class="text-right">' +
            (resultData.A_UNLIMIT_VOL && resultData.A_UNLIMIT_VOL != "0.00"
              ? resultData.A_UNLIMIT_VOL
              : "-") +
            "/" +
            (resultData.CDR_VOL && resultData.CDR_VOL != "0.00"
              ? resultData.CDR_VOL
              : "-") +
            "</td></tr>";
          apitalstructureHtml +=
            '<tr><td></td><td class="text-right table_borderRight"></td><td>境内上市外资股（B股）</td><td class="text-right">' +
            (resultData.B_VOL ? resultData.B_VOL : "-") +
            "</td></tr>";
          apitalstructureHtml += "</tbody>";
          $(".js_capitalstructure .table").html(apitalstructureHtml);
          // 显示股本结构模块 显示股本结构标题表格
          $(".js_capital").show().find(".js_capitalstructure").show();
        } else {
          // 隐藏股本结构模块 隐藏股本结构标题表格
          $(".js_capitalstructure .table").html("");
        }
      },
      errCallback: function () {
        // 隐藏股本结构模块 隐藏股本结构标题表格
        $(".js_capitalstructure .table").html("");
      },
    });
  },
  // 更新股本结构
  updateCapitalstructure: function (seq) {
    var _this = this,
      apitalstructureHtml = "";
    _this.upcapitalstructureParams.seq = seq;
    var apitalstructureNullHtml = "<tbody>";
    apitalstructureNullHtml +=
      '<tr><td colspan="2">股份总数</td><td colspan="2" class="text-right">-</td></tr>';
    apitalstructureNullHtml +=
      '<tr><td colspan="2">境内上市股票合计</td><td colspan="2" class="text-right">-</td></tr>';
    apitalstructureNullHtml +=
      '<tr><td>有限售流通股</td><td class="text-right">-</td>';
    apitalstructureNullHtml +=
      '<td>无限售流通股</td><td class="text-right">-</td></tr>';
    apitalstructureNullHtml +=
      '<tr><td>其中：特别表决权股</td><td class="text-right">-</td>';
    apitalstructureNullHtml +=
      '<td>无限售流通A股/CDR</td><td class="text-right">-</td></tr>';
    apitalstructureNullHtml +=
      '<tr><td></td><td class="text-right"></td><td>境内上市外资股（B股）</td><td class="text-right">-</td></tr>';
    apitalstructureNullHtml += "</tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.upcapitalstructureParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0 && data.result[0]) {
          var resultData = data.result[0];

          // 赋值-数据更新日期
          $(".js_capitalstructure .new_date").html(
            "数据日期：" + resultData.REAL_DATE
          );
          apitalstructureHtml += "<tbody>";
          apitalstructureHtml +=
            '<tr><td colspan="2">股份总数</td><td colspan="2" class="text-right">' +
            (resultData.TOTAL_SHARES ? resultData.TOTAL_SHARES : "-") +
            "</td></tr>";
          apitalstructureHtml +=
            '<tr><td colspan="2">境内上市股票合计</td><td colspan="2" class="text-right">';
          if (resultData.TOTAL_SHARE == "-" && resultData.H_SHARES == "-") {
            apitalstructureHtml += "-";
          } else {
            var TOTAL_SHARE =
              resultData.TOTAL_SHARE && resultData.TOTAL_SHARE != "-"
                ? Number(resultData.TOTAL_SHARE)
                : 0;
            var H_SHARES =
              resultData.H_SHARES && resultData.H_SHARES != "-"
                ? Number(resultData.H_SHARES)
                : 0;
            apitalstructureHtml += (
              (Number(TOTAL_SHARE) - Number(H_SHARES)) /
              10000
            ).toFixed(2);
          }
          apitalstructureHtml += "</td></tr>";
          apitalstructureHtml +=
            '<tr><td>有限售流通股</td><td class="text-right table_borderRight">' +
            (resultData.LIMITED_SHARES ? resultData.LIMITED_SHARES : "-") +
            "</td>";
          apitalstructureHtml += '<td>无限售流通股</td><td class="text-right">';
          if (resultData.A_SHARE == "-" && resultData.B_SHARE == "-") {
            apitalstructureHtml += "-";
          } else {
            var A_SHARE =
              resultData.A_SHARE && resultData.A_SHARE != "-"
                ? Number(resultData.A_SHARE)
                : 0;
            var B_SHARE =
              resultData.B_SHARE && resultData.B_SHARE != "-"
                ? Number(resultData.B_SHARE)
                : 0;
            apitalstructureHtml += ((A_SHARE + B_SHARE) / 10000).toFixed(2);
          }
          apitalstructureHtml += "</td></tr>";
          apitalstructureHtml +=
            '<tr><td>其中：特别表决权股</td><td class="text-right table_borderRight">' +
            (resultData.LISTING_VOTE_SHARES
              ? resultData.LISTING_VOTE_SHARES
              : "-") +
            "</td>";
          apitalstructureHtml +=
            '<td>无限售流通A股/CDR</td><td class="text-right">' +
            (resultData.A_SHARES ? resultData.A_SHARES : "-") +
            "</td></tr>";
          apitalstructureHtml +=
            '<tr><td></td><td class="text-right table_borderRight"></td><td>境内上市外资股（B股）</td><td class="text-right">' +
            (resultData.B_SHARES ? resultData.B_SHARES : "-") +
            "</td></tr>";
          apitalstructureHtml += "</tbody>";
          $(".js_capitalstructure table").html(apitalstructureHtml);
        } else {
          $(".js_capitalstructure table").html(apitalstructureNullHtml);
        }
      },
      errCallback: function () {
        $(".js_capitalstructure table").html(apitalstructureNullHtml);
      },
    });
  },
  //前十大股东
  getTopTenShareholdersList: function () {
    var _this = this;
    var TopTenShareHtml = "<thead><tr>";
    TopTenShareHtml += '<th class="text-left">股东名次</th>';
    TopTenShareHtml += '<th class="text-nowrap">股东名称</th>';
    TopTenShareHtml += '<th class="text-right">持股数量（股）</th>';
    TopTenShareHtml += '<th class="text-right">持股比例（%）</th>';
    TopTenShareHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonSoaQueryUrl,
      data: _this.topTenShareholdersParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $(".js_shareholdertop10").parent().show();
          var dateHtml = data.result[0].REPORT_DATE;
          $(".js_shareholdertop10 .new_date").html('数据日期：' + dateHtml.substring(0, 4) + '年' + dateHtml.substring(5, 7) + '月' + dateHtml.substring(8, 10) + '日');
          $.each(data.result, function (k, v) {
            TopTenShareHtml += "<tr>";
            TopTenShareHtml +=
              '<td class="text-left">' + (k+1) + "</td>";
            TopTenShareHtml +=
              '<td class="text-nowrap">' + v.NAME +
              "</td>";
            TopTenShareHtml +=
              '<td class="text-right">' + v.NUMBER_END + "</td>";
            TopTenShareHtml +=
              '<td class="text-right">' + v.RATIO + "</td>";
            TopTenShareHtml += "</tr>";
          });
          TopTenShareHtml += "</tbody>";
          $(".js_shareholdertop10 .table").html(TopTenShareHtml);
        } else {
          $(".js_shareholdertop10 .table").html("");
          $(".js_shareholdertop10").parent().hide();
        }
      },
      errCallback: function () {
        $(".js_shareholdertop10 .table").html("");
      },
    });
  },
  // 筹资情况-A首次发行
  getAfFinancingList: function () {
    var _this = this;
    var afFinancingHtml = "<thead><tr>"; // A首发表格
    var aaFinancingHtml = "<thead><tr>"; // A增发表格
    var ASF; // A首发
    var AZF; // A增发
    var ASFNum = 0;
    var AZFNum = 0;
    // A首发
    afFinancingHtml += '<th class="text-left">发行数量（万股）</th>';
    afFinancingHtml += '<th class="text-nowrap">发行价格</th>';
    afFinancingHtml += '<th class="text-nowrap">发行日期</th>';
    afFinancingHtml += '<th class="text-nowrap">募集资金总额（万元）</th>';
    afFinancingHtml += '<th class="text-nowrap">发行市盈率（%）</th>';
    afFinancingHtml += '<th class="text-right">发行方式</th>';
    afFinancingHtml += "</tr></thead><tbody>";

    // A增发
    aaFinancingHtml += '<th class="text-left">发行数量（万股）</th>';
    aaFinancingHtml += '<th class="text-nowrap">发行价格</th>';
    aaFinancingHtml += '<th class="text-nowrap">发行日期</th>';
    aaFinancingHtml += '<th class="text-right">发行方式</th>';
    aaFinancingHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.afFinancingParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            ASF = v.ISS_FLAG == "1";
            if (v.ISS_FLAG == "1") {
              ASFNum++;
            } // A首发
            AZF = v.ISS_FLAG == "2";
            if (v.ISS_FLAG == "2") {
              AZFNum++;
            } // A增发
            if (ASF) {
              afFinancingHtml += "<tr>";
              afFinancingHtml +=
                '<td class="text-left">' +
                stringFormatterTwo(v.ISS_VOL) +
                "</td>";
              afFinancingHtml +=
                '<td class="text-nowrap">' +
                stringFormatterTwo(v.ISS_PRICE) +
                "</td>";
              afFinancingHtml +=
                '<td class="text-nowrap">' + dateReplace(v.ISS_DATE) + "</td>";
              afFinancingHtml +=
                '<td class="text-nowrap">' +
                stringFormatterTwo(v.RAISE_CAP) +
                "</td>";
              afFinancingHtml += '<td class="text-nowrap">';
              if (v.ISS_PE == "-") {
                afFinancingHtml += "-";
              } else {
                afFinancingHtml +=
                  v.ISS_PE != "-" ? stringFormatterTwo(v.ISS_PE) : "";
              }
              afFinancingHtml += "</td>";
              afFinancingHtml +=
                '<td class="text-right">' + v.ISS_TYPE_DESC + "</td>";
              afFinancingHtml += "</tr>";
            }
            // A增发
            if (AZF) {
              aaFinancingHtml += "<tr>";
              aaFinancingHtml +=
                '<td class="text-left">' +
                stringFormatterTwo(v.ISS_VOL) +
                "</td>";
              aaFinancingHtml +=
                '<td class="text-nowrap">' +
                stringFormatterTwo(v.ISS_PRICE) +
                "</td>";
              aaFinancingHtml +=
                '<td class="text-nowrap">' + dateReplace(v.ISS_DATE) + "</td>";
              aaFinancingHtml +=
                '<td class="text-right">' + v.ISS_TYPE_DESC + "</td>";
              aaFinancingHtml += "</tr>";
            }
          });

          if (ASFNum) {
            afFinancingHtml += "</tbody>";
            $(".js_affinancing").show().find(".table").html(afFinancingHtml);
            $(".js_financing").show();
          } else {
            $(".js_affinancing .table").html("");
          }
          if (AZFNum) {
            aaFinancingHtml += "</tbody>";
            $(".js_aafinancing .table").html(aaFinancingHtml);
            $(".js_financing").show().find(".js_aafinancing").show();
          } else {
            $(".js_aafinancing .table").html("");
          }
        }
      },
      errCallback: function () {
        $(".js_affinancing .table").html("");
        $(".js_aafinancing .table").html("");
      },
    });
  },
  // 筹资情况-A配股
  getArFinancingList: function () {
    var _this = this;
    var arFinancingHtml = "<thead><tr>";
    arFinancingHtml += '<th class="text-left">股权登记日</th>';
    arFinancingHtml += '<th class="text-nowrap">除权交易日</th>';
    arFinancingHtml += '<th class="text-nowrap">配股价格</th>';
    arFinancingHtml += '<th class="text-nowrap">配股比例(10：?)</th>';
    arFinancingHtml +=
      '<th class="text-nowrap">实际配股量（' + _this.stockUnit + "）</th>";
    arFinancingHtml += '<th class="text-right">配股上市日</th>';
    arFinancingHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.arFinancingParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            arFinancingHtml += "<tr>";
            arFinancingHtml +=
              '<td class="text-left">' + dateReplace(v.REG_DATE) + "</td>";
            arFinancingHtml +=
              '<td class="text-nowrap">' +
              dateReplace(v.DERIGHTS_DATE) +
              "</td>";
            arFinancingHtml +=
              '<td class="text-nowrap">' + v.RIGHTS_PRICE + "</td>";
            arFinancingHtml +=
              '<td class="text-nowrap">' + v.RIGHTS_RATIO + "</td>";
            arFinancingHtml +=
              '<td class="text-nowrap">' + v.RIGHTS_VOL + "</td>";
            arFinancingHtml +=
              '<td class="text-right">' +
              dateReplace(v.RIGHTS_LIST_DATE) +
              "</td>";
            arFinancingHtml += "</tr>";
          });
          arFinancingHtml += "</tbody>";
          $(".js_arfinancing .table").html(arFinancingHtml);
          $(".js_financing").show().find(".js_arfinancing").show();
        } else {
          $(".js_arfinancing .table").html("");
        }
      },
      errCallback: function () {
        $(".js_arfinancing .table").html("");
      },
    });
  },
  // 筹资情况-B首次发行
  getBfFinancingList: function () {
    var _this = this;
    var bfFinancingHtml = "<thead><tr>";
    bfFinancingHtml += '<th class="text-left">发行数量</th>';
    bfFinancingHtml += '<th class="text-nowrap">发行价格(元人民币/美金)</th>';
    bfFinancingHtml += '<th class="text-nowrap">发行时间</th>';
    bfFinancingHtml += '<th class="text-nowrap">筹资总额(万人民币/万美金)</th>';
    bfFinancingHtml += '<th class="text-nowrap">发行市盈率(%)</th>';
    bfFinancingHtml += '<th class="text-nowrap">发行方式</th>';
    bfFinancingHtml += '<th class="text-right">主承销商</th>';
    bfFinancingHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.bfFinancingParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            bfFinancingHtml += "<tr>";
            bfFinancingHtml +=
              '<td class="text-left">' +
              stringFormatter($.trim(v.ISSUED_VOLUME_B)) +
              "</td>";
            bfFinancingHtml +=
              '<td class="text-nowrap">' +
              v.ISSUED_PRICE_B2 +
              "/" +
              v.ISSUED_PRICE_B1 +
              "</td>";
            bfFinancingHtml +=
              '<td class="text-nowrap">' + v.ISSUED_BEGIN_DATE_B + "</td>";
            bfFinancingHtml +=
              '<td class="text-nowrap">' +
              v.RAISED_MONEY_B2 +
              "/" +
              v.RAISED_MONEY_B1 +
              "</td>";
            bfFinancingHtml += '<td class="text-nowrap">';
            if (
              v.ISSUED_PROFIT_RATE_B1 &&
              v.ISSUED_PROFIT_RATE_B1 == "-" &&
              v.ISSUED_PROFIT_RATE_B2 &&
              v.ISSUED_PROFIT_RATE_B2 == "-"
            ) {
              bfFinancingHtml += "-";
            } else {
              bfFinancingHtml +=
                v.ISSUED_PROFIT_RATE_B1 && v.ISSUED_PROFIT_RATE_B1 != "-"
                  ? v.ISSUED_PROFIT_RATE_B1 + "（加权法）"
                  : "";
              bfFinancingHtml +=
                v.ISSUED_PROFIT_RATE_B2 && v.ISSUED_PROFIT_RATE_B2 != "-"
                  ? v.ISSUED_PROFIT_RATE_B2 + "（摊薄法）"
                  : "";
            }
            bfFinancingHtml += "</td>";
            bfFinancingHtml +=
              '<td class="text-nowrap">' + v.ISSUED_MODE_CODE_B + "</td>";
            bfFinancingHtml +=
              '<td class="text-right">' + v.MAIN_UNDERWRITER_NAME_B + "</td>";
            bfFinancingHtml += "</tr>";
          });
          bfFinancingHtml += "</tbody>";
          $(".js_bffinancing .table").html(bfFinancingHtml);
          $(".js_financing").show().find(".js_bffinancing").show();
        } else {
          $(".js_bffinancing .table").html("");
        }
      },
      errCallback: function () {
        $(".js_bffinancing .table").html("");
      },
    });
  },
  // 筹资情况-B增发
  getBaFinancingList: function () {
    var _this = this;
    var baFinancingHtml = "<thead><tr>";
    baFinancingHtml += '<th class="text-left">发行数量</th>';
    baFinancingHtml += '<th class="text-nowrap">发行价格(元人民币/美金)</th>';
    baFinancingHtml += '<th class="text-nowrap">发行日期</th>';
    baFinancingHtml += '<th class="text-nowrap">筹资总额(万人民币/万美金)</th>';
    baFinancingHtml += '<th class="text-nowrap">发行方式</th>';
    baFinancingHtml += '<th class="text-nowrap">国际协调人</th>';
    baFinancingHtml += '<th class="text-right">主承销商</th>';
    baFinancingHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.baFinancingParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            baFinancingHtml += "<tr>";
            baFinancingHtml +=
              '<td class="text-left">' +
              stringFormatter($.trim(v.ISSUED_VOLUME_B)) +
              "</td>";
            baFinancingHtml +=
              '<td class="text-nowrap">' +
              v.ISSUED_PRICE_B2 +
              "/" +
              v.ISSUED_PRICE_B1 +
              "</td>";
            baFinancingHtml +=
              '<td class="text-nowrap">' + v.ISSUED_BEGIN_DATE_B + "</td>";
            baFinancingHtml +=
              '<td class="text-nowrap">' +
              v.RAISED_MONEY_B2 +
              "/" +
              v.RAISED_MONEY_B1 +
              "</td>";
            baFinancingHtml +=
              '<td class="text-nowrap">' + v.ISSUED_MODE_CODE_B + "</td>";
            baFinancingHtml += '<td class="text-nowrap">';
            if (
              v.ISSUED_PROFIT_RATE_B1 &&
              v.ISSUED_PROFIT_RATE_B1 == "-" &&
              v.ISSUED_PROFIT_RATE_B2 &&
              v.ISSUED_PROFIT_RATE_B2 == "-"
            ) {
              baFinancingHtml += "-";
            } else {
              baFinancingHtml +=
                v.ISSUED_PROFIT_RATE_B1 && v.ISSUED_PROFIT_RATE_B1 != "-"
                  ? v.ISSUED_PROFIT_RATE_B1 + "（加权法）"
                  : "";
              baFinancingHtml +=
                v.ISSUED_PROFIT_RATE_B2 && v.ISSUED_PROFIT_RATE_B2 != "-"
                  ? v.ISSUED_PROFIT_RATE_B2 + "（摊薄法）"
                  : "";
            }
            baFinancingHtml += "</td>";
            baFinancingHtml +=
              '<td class="text-nowrap">' + v.COORDINATOR_B + "</td>";
            baFinancingHtml +=
              '<td class="text-right">' + v.MAIN_UNDERWRITER_NAME_B + "</td>";
            baFinancingHtml += "</tr>";
          });
          baFinancingHtml += "</tbody>";
          $(".js_bafinancing .table").html(baFinancingHtml);
          $(".js_financing").show().find(".js_bafinancing").show();
        } else {
          $(".js_bafinancing .table").html("");
        }
      },
      errCallback: function () {
        $(".js_bafinancing .table").html("");
      },
    });
  },
  // 筹资情况-B配股
  getBrFinancingList: function () {
    var _this = this;
    var brFinancingHtml = "<thead><tr>";
    brFinancingHtml += '<th class="text-left">股权登记日</th>';
    brFinancingHtml += '<th class="text-nowrap">除权基准日</th>';
    brFinancingHtml += '<th class="text-nowrap">最后交易日</th>';
    brFinancingHtml += '<th class="text-nowrap">股配股价格</th>';
    brFinancingHtml += '<th class="text-nowrap">美元汇率</th>';
    brFinancingHtml += '<th class="text-nowrap">配股比例(10：?)</th>';
    brFinancingHtml += '<th class="text-nowrap">配股缴款起始日</th>';
    brFinancingHtml += '<th class="text-nowrap">配股缴款截止日</th>';
    brFinancingHtml += '<th class="text-nowrap">实际配股量(万股)</th>';
    brFinancingHtml += '<th class="text-nowrap">配股上市日</th>';
    brFinancingHtml += '<th class="text-right">配股类别说明</th>';
    brFinancingHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.brFinancingParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            brFinancingHtml += "<tr>";
            brFinancingHtml +=
              '<td class="text-left">' + v.RECORD_DATE_B + "</td>";
            brFinancingHtml +=
              '<td class="text-nowrap">' + v.EX_RIGHTS_DATE_B + "</td>";
            brFinancingHtml +=
              '<td class="text-nowrap">' + v.LAST_TRADE_DATE_B + "</td>";
            brFinancingHtml +=
              '<td class="text-nowrap">' +
              stringFormatter($.trim(v.PRICE_OF_RIGHTS_ISSUE_B)) +
              "</td>";
            brFinancingHtml +=
              '<td class="text-nowrap">' + v.EXCHANGE_RATE + "</td>";
            brFinancingHtml +=
              '<td class="text-nowrap">' + v.RATIO_OF_RIGHTS_ISSUE_B + "</td>";
            brFinancingHtml +=
              '<td class="text-nowrap">' +
              v.START_DATE_OF_REMITTANCE_B +
              "</td>";
            brFinancingHtml +=
              '<td class="text-nowrap">' + v.END_DATE_OF_REMITTANCE_B + "</td>";
            brFinancingHtml +=
              '<td class="text-nowrap">' + v.TRUE_COLUME_B + "</td>";
            brFinancingHtml +=
              '<td class="text-nowrap">' + v.LISTING_DATE_B + "</td>";
            brFinancingHtml +=
              '<td class="text-right">' + v.RIGHTS_TYPE + "</td>";
            brFinancingHtml += "</tr>";
          });
          brFinancingHtml += "</tbody>";
          $(".js_brfinancing .table").html(brFinancingHtml);
          $(".js_financing").show().find(".js_brfinancing").show();
        } else {
          $(".js_brfinancing .table").html("");
        }
      },
      errCallback: function () {
        $(".js_brfinancing .table").html("");
      },
    });
  },
  // 筹资情况-科创板首发/增发
  getSfFinancingList: function () {
    var _this = this;
    var sfFinancingHtml = "<thead><tr>"; // 首发表格
    var saFinancingHtml = "<thead><tr>"; // 增发表格
    var kcbSF;
    var kcbZF;
    var kcbSFNum = 0;
    var kcbZFNum = 0;
    // 首发
    sfFinancingHtml +=
      '<th class="text-left">发行数量（' + _this.stockUnit + "）</th>";
    sfFinancingHtml +=
      '<th class="text-nowrap">其中：战投获配数量（' +
      _this.stockUnit +
      "）</th>";
    sfFinancingHtml += '<th class="text-nowrap">发行价格</th>';
    sfFinancingHtml += '<th class="text-nowrap">发行日期</th>';
    sfFinancingHtml += '<th class="text-nowrap">募集资金总额（万元）</th>';
    sfFinancingHtml += '<th class="text-nowrap">发行市盈率（%）</th>';
    sfFinancingHtml += '<th class="text-right">发行方式</th>';
    sfFinancingHtml += "</tr></thead><tbody>";

    // 增发
    saFinancingHtml +=
      '<th class="text-left">发行数量（' + _this.stockUnit + "）</th>";
    saFinancingHtml += '<th class="text-nowrap">发行价格</th>';
    saFinancingHtml += '<th class="text-nowrap">发行日期</th>';
    saFinancingHtml += '<th class="text-right">发行方式</th>';
    saFinancingHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.sfFinancingParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            kcbSF = v.ISS_FLAG == "1";
            if (v.ISS_FLAG == "1") {
              kcbSFNum++;
            } // 科创板首发
            kcbZF = v.ISS_FLAG == "2";
            if (v.ISS_FLAG == "2") {
              kcbZFNum++;
            }
            if (kcbSF) {
              // 科创板首发
              sfFinancingHtml += "<tr>";
              sfFinancingHtml +=
                '<td class="text-left">' +
                stringFormatterTwo(v.ISS_VOL) +
                "</td>";
              sfFinancingHtml +=
                '<td class="text-nowrap">' +
                stringFormatter(v.INVESTOR_VOL) +
                "</td>";
              sfFinancingHtml +=
                '<td class="text-nowrap">' +
                stringFormatterTwo(v.ISS_PRICE) +
                "</td>";
              sfFinancingHtml +=
                '<td class="text-nowrap">' + dateReplace(v.ISS_DATE) + "</td>";
              sfFinancingHtml +=
                '<td class="text-nowrap">' +
                stringFormatterTwo(v.RAISE_CAP) +
                "</td>";
              sfFinancingHtml += '<td class="text-nowrap">';
              if (v.ISSUE_PE_RATIO && v.ISSUE_PE_RATIO == "-") {
                sfFinancingHtml += "-";
              } else {
                sfFinancingHtml +=
                  v.ISS_PE != "-" ? stringFormatterTwo(v.ISS_PE) : "";
              }
              sfFinancingHtml += "</td>";
              sfFinancingHtml +=
                '<td class="text-right">' + v.ISS_TYPE_DESC + "</td>";
              sfFinancingHtml += "</tr>";
            }

            if (kcbZF) {
              // 科创板增发
              saFinancingHtml += "<tr>";
              saFinancingHtml +=
                '<td class="text-left">' +
                stringFormatterTwo(v.ISS_VOL) +
                "</td>";
              saFinancingHtml +=
                '<td class="text-nowrap">' +
                stringFormatterTwo(v.ISS_PRICE) +
                "</td>";
              saFinancingHtml +=
                '<td class="text-nowrap">' + dateReplace(v.ISS_DATE) + "</td>";
              saFinancingHtml +=
                '<td class="text-right">' + v.ISS_TYPE_DESC + "</td>";
              saFinancingHtml += "</tr>";
            }
          });
          if (kcbSFNum) {
            sfFinancingHtml += "</tbody>";
            $(".js_sffinancing .table").html(sfFinancingHtml);
            $(".js_financing").show().find(".js_sffinancing").show();
          } else {
            $(".js_sffinancing .table").html("");
          }
          if (kcbZFNum) {
            saFinancingHtml += "</tbody>";
            $(".js_safinancing .table").html(saFinancingHtml);
            $(".js_financing").show().find(".js_safinancing").show();
          } else {
            $(".js_safinancing .table").html("");
          }
        }
      },
      errCallback: function () {
        $(".js_sffinancing .table").html("");
        $(".js_safinancing .table").html("");
      },
    });
  },
  // 筹资情况-科创板配股
  getSrFinancingList: function () {
    var _this = this;
    var srFinancingHtml = "<thead><tr>";
    srFinancingHtml += '<th class="text-left">股权登记日</th>';
    srFinancingHtml += '<th class="text-nowrap">除权交易日</th>';
    srFinancingHtml += '<th class="text-nowrap">配股价格</th>';
    srFinancingHtml += '<th class="text-nowrap">配股比例(10：?)</th>';
    srFinancingHtml +=
      '<th class="text-nowrap">实际配股量（' + _this.stockUnit + "）</th>";
    srFinancingHtml += '<th class="text-right">配股上市日</th>';
    srFinancingHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.srFinancingParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            srFinancingHtml += "<tr>";
            srFinancingHtml +=
              '<td class="text-left">' + dateReplace(v.REG_DATE) + "</td>";
            srFinancingHtml +=
              '<td class="text-nowrap">' +
              dateReplace(v.DERIGHTS_DATE) +
              "</td>";
            srFinancingHtml +=
              '<td class="text-nowrap">' + v.RIGHTS_PRICE + "</td>";
            srFinancingHtml +=
              '<td class="text-nowrap">' + v.RIGHTS_RATIO + "</td>";
            srFinancingHtml +=
              '<td class="text-nowrap">' + v.RIGHTS_VOL + "</td>";
            srFinancingHtml +=
              '<td class="text-right">' +
              dateReplace(v.RIGHTS_LIST_DATE) +
              "</td>";
            srFinancingHtml += "</tr>";
          });
          srFinancingHtml += "</tbody>";
          $(".js_srfinancing .table").html(srFinancingHtml);
          $(".js_financing").show().find(".js_srfinancing").show();
        } else {
          $(".js_srfinancing .table").html("");
        }
      },
      errCallback: function () {
        $(".js_srfinancing .table").html("");
      },
    });
  },
  // 获取首日表现
  getFristDayList: function () {
    var _this = this;
    var fristDayHtml = "<thead><tr>";
    fristDayHtml += '<th class="text-nowrap">日期</th>';
    fristDayHtml += '<th class="text-nowrap">事件</th>';
    fristDayHtml +=
      '<th class="text-right">当日流通股本（' + _this.stockUnit + "）</th>";
    fristDayHtml += '<th class="text-right">开盘价(元)</th>';
    fristDayHtml += '<th class="text-right">最高价(元)</th>';
    fristDayHtml += '<th class="text-right">最低价(元)</th>';
    fristDayHtml += '<th class="text-right">收盘价(元)</th>';
    fristDayHtml +=
      '<th class="text-right">成交量（' + _this.stockUnit + "）</th>";
    fristDayHtml += '<th class="text-right">成交额(万元)</th>';
    fristDayHtml += '<th class="text-right">换手率(%)</th>';
    fristDayHtml += "</tr></thead><tbody>";
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.fristDayParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $.each(data.result, function (k, v) {
            fristDayHtml += "<tr>";
            fristDayHtml +=
              '<td class="text-nowrap">' + dateReplace(v.TRADE_DATE) + "</td>";
            fristDayHtml +=
              '<td class="text-nowrap">' + v.ISSUE_TYPE_DESC + "</td>";
            fristDayHtml += '<td class="text-right">' + v.NEGO_VOL + "</td>";
            fristDayHtml += '<td class="text-right">' + v.OPEN_PRICE + "</td>";
            fristDayHtml += '<td class="text-right">' + v.HIGH_PRICE + "</td>";
            fristDayHtml += '<td class="text-right">' + v.LOW_PRICE + "</td>";
            fristDayHtml += '<td class="text-right">' + v.CLOSE_PRICE + "</td>";
            fristDayHtml += '<td class="text-right">' + v.TRADE_VOL + "</td>";
            fristDayHtml += '<td class="text-right">' + v.TRADE_AMT + "</td>";
            fristDayHtml +=
              '<td class="text-right">' +
              stringFormatter($.trim(v.TO_RATE)) +
              "</td>";
            fristDayHtml += "</tr>";
          });
          fristDayHtml += "</tbody>";
          $(".js_fristDay .table").html(fristDayHtml);
          $(".js_financing").show().find(".js_fristDay").show();
        } else {
          $(".js_fristDay .table").html("");
        }
      },
      errCallback: function () {
        $(".js_fristDay .table").html("");
      },
    });
  },
  // 利润分配-分红-A股
  getAbProfitList: function (pageIndex) {
    var _this = this;
    if (!paginationChange(_this.abprofitParams, pageIndex, ".js_abprofit"))
      return; // 触发分页改变分页参数
    var abprofitHtml = "<thead><tr>";
    abprofitHtml += '<th class="text-left">股权登记日</th>';
    abprofitHtml +=
      '<th class="text-nowrap">股权登记日总股本（' +
      _this.stockUnit +
      "）</th>";
    abprofitHtml += '<th class="text-nowrap">除息交易日</th>';
    abprofitHtml += '<th class="text-right">每' + _this.gfUnit + "红利</th>";
    abprofitHtml += "</tr></thead><tbody>";
    _this.abprofitParams["pageHelp.pageSize"] = 5;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.abprofitParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $(".js_abprofit .title_lev3").html(
            'A股<span class="new_date">* 单位:元人民币</span>'
          );
          $.each(data.result, function (k, v) {
            abprofitHtml += "<tr>";
            abprofitHtml +=
              '<td class="text-left">' + dateReplace(v.A_REG_DATE) + "</td>";
            abprofitHtml +=
              '<td class="text-nowrap">' + v.A_STOCK_VOL + "</td>";
            abprofitHtml +=
              '<td class="text-nowrap">' + dateReplace(v.A_DIV_DATE) + "</td>";
            abprofitHtml += '<td class="text-right">';
            abprofitHtml +=
              v.A_BEFR_TAX_DIV && v.A_BEFR_TAX_DIV != "-"
                ? v.A_BEFR_TAX_DIV + "(含税红利)"
                : "";
            abprofitHtml += "</td>";
            abprofitHtml += "</tr>";
          });
          abprofitHtml += "</tbody>";
          $(".js_abprofit .table").html(abprofitHtml);
          Page.navigation(
            ".js_abprofit .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getAbProfitList"
          );
          $(".js_abprofit .pagination-box .change-pageSize").remove();
          $(".js_profit")
            .show()
            .find(".js_fhprofit")
            .show()
            .find(".js_abprofit")
            .show();
        } else {
          $(".js_abprofit .table").html("");
          $(".js_abprofit .pagination-box").html("");
        }
      },
      errCallback: function () {
        $(".js_abprofit .table").html("");
        $(".js_abprofit .pagination-box").html("");
      },
    });
  },
  // 利润分配-分红-科创板
  getSbProfitList: function (pageIndex) {
    var _this = this;
    if (!paginationChange(_this.sbprofitParams, pageIndex, ".js_abprofit"))
      return; // 触发分页改变分页参数
    var sbprofitHtml = "<thead><tr>";
    sbprofitHtml += '<th class="text-left">股权登记日</th>';
    sbprofitHtml +=
      '<th class="text-nowrap">股权登记日总股本（' +
      _this.stockUnit +
      "）</th>";
    sbprofitHtml += '<th class="text-nowrap">除息交易日</th>';
    sbprofitHtml += '<th class="text-right">每' + _this.gfUnit + "红利</th>";
    sbprofitHtml += "</tr></thead><tbody>";
    _this.sbprofitParams["pageHelp.pageSize"] = 5;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.sbprofitParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $(".js_abprofit .title_lev3").html(
            '科创板<span class="new_date">* 单位:元人民币</span>'
          );
          $.each(data.result, function (k, v) {
            sbprofitHtml += "<tr>";
            sbprofitHtml +=
              '<td class="text-left">' + dateReplace(v.A_REG_DATE) + "</td>";
            sbprofitHtml +=
              '<td class="text-nowrap">' + v.A_STOCK_VOL + "</td>";
            sbprofitHtml +=
              '<td class="text-nowrap">' + dateReplace(v.A_DIV_DATE) + "</td>";
            sbprofitHtml += '<td class="text-right">';
            sbprofitHtml +=
              v.A_BEFR_TAX_DIV && v.A_BEFR_TAX_DIV != "-"
                ? v.A_BEFR_TAX_DIV + "(含税红利)"
                : "";
            sbprofitHtml += "</td>";
            sbprofitHtml += "</tr>";
          });
          sbprofitHtml += "</tbody>";
          $(".js_abprofit .table").html(sbprofitHtml);
          Page.navigation(
            ".js_abprofit .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getSbProfitList"
          );
          $(".js_abprofit .pagination-box .change-pageSize").remove();
          $(".js_profit")
            .show()
            .find(".js_fhprofit")
            .show()
            .find(".js_abprofit")
            .show();
        }
      },
      errCallback: function () {
        $(".js_abprofit .table").html("");
        $(".js_abprofit .pagination-box").html("");
      },
    });
  },
  // 利润分配-分红-B股
  getBbProfitList: function (pageIndex) {
    var _this = this;
    if (!paginationChange(_this.bbprofitParams, pageIndex, ".js_bbprofit"))
      return; // 触发分页改变分页参数
    var bbprofitHtml = "<thead><tr>";
    bbprofitHtml += '<th class="text-left">股权登记日</th>';
    bbprofitHtml +=
      '<th class="text-nowrap">股权登记日总股本（' +
      _this.stockUnit +
      "）</th>";
    bbprofitHtml += '<th class="text-nowrap">除息交易日</th>';
    bbprofitHtml += '<th class="text-right">每' + _this.gfUnit + "红利</th>";
    bbprofitHtml += "</tr></thead><tbody>";
    _this.bbprofitParams["pageHelp.pageSize"] = 5;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.bbprofitParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $(".js_bbprofit")
            .prev(".title_lev3")
            .find("span")
            .eq(0)
            .html(
              "B股 " +
              data.result[0].SECURITY_NAME_B +
              "(" +
              data.result[0].SECURITY_CODE_B +
              ")"
            );
          $.each(data.result, function (k, v) {
            bbprofitHtml += "<tr>";
            bbprofitHtml +=
              '<td class="text-left">' + dateReplace(v.B_REG_DATE) + "</td>";
            bbprofitHtml +=
              '<td class="text-nowrap">' + v.B_STOCK_VOL + "</td>";
            bbprofitHtml +=
              '<td class="text-nowrap">' + dateReplace(v.B_DIV_DATE) + "</td>";
            bbprofitHtml += '<td class="text-right">';
            bbprofitHtml +=
              v.B_BEFR_TAX_DIV && v.B_BEFR_TAX_DIV != "-"
                ? v.B_BEFR_TAX_DIV + "(含税红利)"
                : "";
            bbprofitHtml += "</td>";
            bbprofitHtml += "</tr>";
          });
          bbprofitHtml += "</tbody>";
          $(".js_bbprofit .table").html(bbprofitHtml);
          Page.navigation(
            ".js_bbprofit .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getBbProfitList"
          );
          $(".js_bbprofit .pagination-box .change-pageSize").remove();
          $(".js_profit")
            .show()
            .find(".js_fhprofit")
            .show()
            .find(".js_bbprofit")
            .show();
        } else {
          $(".js_bbprofit .table").html("");
          $(".js_bbprofit .pagination-box").html("");
        }
      },
      errCallback: function () {
        $(".js_bbprofit .table").html("");
        $(".js_bbprofit .pagination-box").html("");
      },
    });
  },
  // 利润分配-送股-A股
  getAgProfitList: function (pageIndex) {
    var _this = this;
    if (!paginationChange(_this.agprofitParams, pageIndex, ".js_agprofit"))
      return; // 触发分页改变分页参数
    var agprofitHtml = "<thead><tr>";
    agprofitHtml += '<th class="text-left">股权登记日</th>';
    agprofitHtml +=
      '<th class="text-nowrap">股权登记日总股本（' +
      _this.stockUnit +
      "）</th>";
    agprofitHtml += '<th class="text-nowrap">除权基准日</th>';
    agprofitHtml += '<th class="text-nowrap">送股比例</th>';
    agprofitHtml += '<th class="text-right">转股比例</th>';
    agprofitHtml += "</tr></thead><tbody>";
    _this.agprofitParams["pageHelp.pageSize"] = 5;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.agprofitParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $(".js_agprofit")
            .prev(".title_lev3")
            .html(
              _this.ashareUnit +
              " " +
              data.result[0].SECURITY_NAME_A +
              "(" +
              data.result[0].SECURITY_CODE_A +
              ")送股"
            );
          $.each(data.result, function (k, v) {
            agprofitHtml += "<tr>";
            agprofitHtml +=
              '<td class="text-left">' + dateReplace(v.A_REG_DATE) + "</td>";
            agprofitHtml += '<td class="text-nowrap">' + v.ISSUE_VOL + "</td>";
            agprofitHtml +=
              '<td class="text-nowrap">' +
              dateReplace(v.A_DERIGHTS_DATE) +
              "</td>";
            agprofitHtml +=
              '<td class="text-nowrap">' + v.BONUS_RATIO + "</td>";
            agprofitHtml +=
              '<td class="text-right">' + v.CONVERT_RATIO + "</td>";
            agprofitHtml += "</tr>";
          });
          agprofitHtml += "</tbody>";
          $(".js_agprofit .table").html(agprofitHtml);
          Page.navigation(
            ".js_agprofit .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getAgProfitList"
          );
          $(".js_agprofit .pagination-box .change-pageSize").remove();
          $(".js_profit")
            .show()
            .find(".js_sgprofit")
            .show()
            .find(".js_agprofit")
            .show();
        } else {
          $(".js_agprofit .table").html("");
          $(".js_agprofit .pagination-box").html("");
        }
        // b送股
        if (data && data.result && data.result.length > 0) {
          if (
            data.result[0].B_STOCK_CODE &&
            data.result[0].B_STOCK_CODE != "-"
          ) {
            var bgprofitHtml = "<thead><tr>";
            bgprofitHtml += '<th class="text-left">股权登记日</th>';
            bgprofitHtml +=
              '<th class="text-nowrap">股权登记日总股本（' +
              _this.stockUnit +
              "）</th>";
            bgprofitHtml += '<th class="text-nowrap">除权基准日</th>';
            bgprofitHtml += '<th class="text-nowrap">送股比例</th>';
            bgprofitHtml += '<th class="text-right">转股比例</th>';
            bgprofitHtml += "</tr></thead><tbody>";
            $(".js_bgprofit")
              .prev(".title_lev3")
              .html(
                _this.bshareUnit +
                " " +
                data.result[0].SECURITY_NAME_B +
                "(" +
                data.result[0].SECURITY_CODE_B +
                ")送股"
              );
            $.each(data.result, function (k, v) {
              bgprofitHtml += "<tr>";
              bgprofitHtml +=
                '<td class="text-left">' + dateReplace(v.B_REG_DATE) + "</td>";
              bgprofitHtml +=
                '<td class="text-nowrap">' + v.ISSUE_VOL + "</td>";
              bgprofitHtml +=
                '<td class="text-nowrap">' +
                dateReplace(v.B_DERIGHTS_DATE) +
                "</td>";
              bgprofitHtml +=
                '<td class="text-nowrap">' + v.BONUS_RATIO + "</td>";
              bgprofitHtml +=
                '<td class="text-right">' + v.CONVERT_RATIO + "</td>";
              bgprofitHtml += "</tr>";
            });
            bgprofitHtml += "</tbody>";
            $(".js_bgprofit .table").html(bgprofitHtml);
            Page.navigation(
              ".js_bgprofit .pagination-box",
              data.pageHelp.pageCount,
              data.pageHelp.total,
              data.pageHelp.pageNo,
              data.pageHelp.pageSize,
              "company.getBgProfitList"
            );
            $(".js_bgprofit .pagination-box .change-pageSize").remove();
            $(".js_profit")
              .show()
              .find(".js_sgprofit")
              .show()
              .find(".js_bgprofit")
              .show();
          }
        } else {
          $(".js_bgprofit .table").html("");
          $(".js_bgprofit .pagination-box").html("");
        }
      },
      errCallback: function () {
        $(".js_agprofit .table").html("");
        $(".js_agprofit .pagination-box").html("");
      },
    });
  },
  // 利润分配-送股-科创板
  getSgProfitList: function (pageIndex) {
    var _this = this;
    if (!paginationChange(_this.sgprofitParams, pageIndex, ".js_sgprofit"))
      return; // 触发分页改变分页参数
    var akprofitHtml = "<thead><tr>";
    akprofitHtml += '<th class="text-left">股权登记日</th>';
    akprofitHtml +=
      '<th class="text-nowrap">股权登记日总股本（' +
      _this.stockUnit +
      "）</th>";
    akprofitHtml += '<th class="text-nowrap">除权基准日</th>';
    akprofitHtml += '<th class="text-nowrap">送股比例</th>';
    akprofitHtml += '<th class="text-right">转股比例</th>';
    akprofitHtml += "</tr></thead><tbody>";
    _this.sgprofitParams["pageHelp.pageSize"] = 5;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.sgprofitParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          $(".js_agprofit h2").html("科创板");
          $.each(data.result, function (k, v) {
            akprofitHtml += "<tr>";
            akprofitHtml +=
              '<td class="text-left">' + dateReplace(v.A_REG_DATE) + "</td>";
            akprofitHtml +=
              '<td class="text-nowrap">' +
              stringFormatter($.trim(v.ISSUE_VOL)) +
              "</td>";
            akprofitHtml +=
              '<td class="text-nowrap">' +
              dateReplace(v.A_DERIGHTS_DATE) +
              "</td>";
            akprofitHtml +=
              '<td class="text-nowrap">' + v.BONUS_RATIO + "</td>";
            akprofitHtml +=
              '<td class="text-right">' + v.CONVERT_RATIO + "</td>";
            akprofitHtml += "</tr>";
          });
          akprofitHtml += "</tbody>";
          $(".js_sgprofit .table").html(akprofitHtml);
          Page.navigation(
            ".js_agprofit .pagination-box",
            data.pageHelp.pageCount,
            data.pageHelp.total,
            data.pageHelp.pageNo,
            data.pageHelp.pageSize,
            "company.getSgProfitList"
          );
          $(".js_sgprofit .pagination-box .change-pageSize").remove();
          $(".js_profit")
            .show()
            .find(".js_sgprofit")
            .show()
            .find(".js_agprofit")
            .show();
        } else {
          $(".js_sgprofit .table").html("");
          $(".js_sgprofit .pagination-box").html("");
        }
      },
      errCallback: function () {
        $(".js_sgprofit .table").html("");
        $(".js_sgprofit .pagination-box").html("");
      },
    });
  },
  getCompanyOverviewNullHtml: function () {
    var _this = this,
      tableHtml = "<tbody>";
    tableHtml += "<tr><td>证券代码*</td><td>-/-/-</td></tr>";
    tableHtml += "<tr><td>证券简称</td><td>-</td></tr>";
    tableHtml += "<tr><td>扩位证券简称</td><td>-</td></tr>";
    tableHtml += "<tr><td>上市日*</td><td>-/-/-</td></tr>";
    tableHtml +=
      '<tr><td class="text-nowrap">可转债简称（代码）/<br>可转债转股简称（代码）</td><td>-</td></tr>';
    tableHtml += "<tr><td>公司全称(中/英)</td><td>--</td></tr>";
    tableHtml += "<tr><td>注册地址</td><td>-</td></tr>";
    tableHtml += "<tr><td>通讯地址（邮编）</td><td>-(-)</td></tr>";
    tableHtml += "<tr><td>法定代表人</td><td>-</td></tr>";
    tableHtml += "<tr><td>董事会秘书姓名</td><td>-</td></tr>";
    tableHtml += "<tr><td>E-mail</td><td>-</td></tr>";
    tableHtml += "<tr><td>联系电话</td><td>-</td></tr>";
    tableHtml += "<tr><td>行业分类**</td><td>-/-/-</td></tr>";
    tableHtml += "<tr><td>所属省/直辖市</td><td>-</td></tr>";
    tableHtml += "<tr><td>状态*</td><td>-</td></tr>";
    tableHtml += "<tr><td>是否上证180样本股</td><td>-</td></tr>";
    tableHtml += "<tr><td>是否境外上市</td><td>-</td></tr>";
    tableHtml += "<tr><td>境外上市地</td><td>-</td></tr>";
    $(".js_companyOverview table").html(tableHtml);
  },
  // 拼接公司概况HTML
  getCompanyOverviewHtml: function () {
    var _this = this;
    var aListDate = ""; // A股上市日期
    var aCode = ""; // A股股票代码
    var aStatus = ""; // A股状态
    var bListDate = ""; // B股上市日期
    var bCode = ""; // B股股票代码
    var bStatus = ""; // B股状态
    var cdrListDate = ""; // 科创板cdr上市日期
    var cdrCode = ""; // cdr股票代码
    var cdrStatus = ""; // cdr股状态
    (tableHtml = "<tbody>"), (data = _this.companyOverviewData); // 公司概况数据

    if (data.SEC_TYPE != "科创CDR") {
      aListDate = data.A_LIST_DATE != "-" ? dateReplace(data.A_LIST_DATE) : "-";
      aCode = data.A_STOCK_CODE;
      aStatus = data.STATE_CODE_A_DESC;
    } else if (data.SEC_TYPE == "科创CDR") {
      cdrListDate =
        data.A_LIST_DATE != "-" ? dateReplace(data.A_LIST_DATE) : "-";
      cdrCode = data.A_STOCK_CODE;
      cdrStatus = data.STATE_CODE_A_DESC;
    }

    bListDate = data.B_LIST_DATE != "-" ? dateReplace(data.B_LIST_DATE) : "-";
    bCode = data.B_STOCK_CODE;
    bStatus = data.STATE_CODE_B_DESC;

    // 证券代码
    tableHtml +=
      "<tr><td>证券代码*</td><td>" +
      (aCode ? aCode : "-") +
      "/" +
      (bCode ? bCode : "-") +
      "/" +
      (cdrCode ? cdrCode : "-") +
      "</td></tr>";

    tableHtml +=
      "<tr><td>证券简称</td><td>" +
      data.SECURITY_ABBR_A_CN +
      kcbShowIconFlag(data.STATUS_A) +
      "/" +
      data.SECURITY_ABBR_B_CN +
      "</td></tr>";
    tableHtml +=
      "<tr><td>扩位证券简称</td><td>" +
      data.SEC_NAME_FULL +
      "/" +
      data.SECURITY_ABBR_B_FULL +
      "</td></tr>";
    // 上市日期处理
    tableHtml += "<tr><td>上市日*</td><td>";
    tableHtml +=
      (aListDate ? aListDate : "-") +
      "/" +
      bListDate +
      "/" +
      (cdrListDate ? cdrListDate : "-");
    tableHtml += "</td></tr>";
    tableHtml +=
      '<tr><td class="text-nowrap">可转债简称（代码）/<br>可转债转股简称（代码）</td><td>';
    var allBondData = [
      data.CHANGEABLE_BOND_ABBR,
      data.CHANGEABLE_BOND_CODE,
      data.OTHER_ABBR,
      data.OTHER_CODE,
    ],
      babbr = data.CHANGEABLE_BOND_ABBR
        ? data.CHANGEABLE_BOND_ABBR.split(",")
        : [],
      bcode = data.CHANGEABLE_BOND_CODE
        ? data.CHANGEABLE_BOND_CODE.split(",")
        : [],
      oabbr = data.OTHER_ABBR ? data.OTHER_ABBR.split(",") : [],
      ocode = data.OTHER_CODE ? data.OTHER_CODE.split(",") : [];
    allBondData.sort(function (a, b) {
      return b.split(",").length - a.split(",").length;
    });
    var maxLength = allBondData[0].split(",").length;
    for (var i = 0; i < maxLength; i++) {
      if (!babbr[i]) babbr[i] = "-";
      if (!bcode[i]) bcode[i] = "-";
      if (!oabbr[i]) oabbr[i] = "-";
      if (!ocode[i]) ocode[i] = "-";
      if (i != maxLength - 1) {
        tableHtml +=
          babbr[i] +
          "(" +
          bcode[i] +
          ")/" +
          oabbr[i] +
          "(" +
          ocode[i] +
          "),<br>";
      } else {
        tableHtml +=
          babbr[i] + "(" + bcode[i] + ")/" + oabbr[i] + "(" + ocode[i] + ")";
      }
    }
    tableHtml += "</td></tr>";

    tableHtml +=
      "<tr><td>公司全称(中/英)</td><td>" +
      data.FULL_NAME +
      "/" +
      data.FULL_NAME_EN +
      "</td></tr>";
    // 科创板
    if (data.SEC_TYPE == "科创A" || data.SEC_TYPE == "科创CDR") {
      tableHtml += "<tr><td>上市时是否盈利</td><td>";
      if (data.PROFIT_FLAG == "N") {
        tableHtml += "否";
      } else if (data.PROFIT_FLAG == "Y") {
        tableHtml += "是";
      } else {
        tableHtml += data.PROFIT_FLAG;
      }
      tableHtml += "</td></tr>";

      tableHtml += "<tr><td>是否具有表决权差异安排</td><td>";
      if (data.IS_VOTE_DIFF == "N") {
        tableHtml += "否";
      } else if (data.IS_VOTE_DIFF == "Y") {
        tableHtml += "是（W）";
      } else {
        tableHtml += data.IS_VOTE_DIFF;
      }
      tableHtml += "</td></tr>";
    }
    tableHtml += "<tr><td>注册地址</td><td>" + data.REG_ADDRESS + "</td></tr>";
    tableHtml +=
      "<tr><td>通讯地址（邮编）</td><td>" +
      data.OFFICE_ADDRESS +
      "(" +
      data.OFFICE_ZIP +
      ")</td></tr>";
    tableHtml +=
      "<tr><td>法定代表人</td><td>" + data.LEGAL_REPRESENTATIVE + "</td></tr>";
    tableHtml += "<tr><td>董事会秘书姓名</td><td>" + data.NAME + "</td></tr>";
    tableHtml +=
      '<tr><td>E-mail</td><td><a target="_blank" href="mailto:' +
      data.E_MAIL_ADDRESS +
      '">' +
      data.E_MAIL_ADDRESS +
      "</a></td></tr>";
    tableHtml +=
      "<tr><td>联系电话</td><td>" + data.INVESTOR_PHONE + "</td></tr>";
    // 更新 科创板 行业 SMALL_CLASS_NAME
    if (_this.stockType == "1") {
      tableHtml +=
        "<tr><td>CSRC行业</td><td>" + data.SMALL_CLASS_NAME + "</td></tr>";
    } else {
      tableHtml +=
        "<tr><td>行业分类**</td><td>" +
        data.CSRC_CODE_DESC +
        "/" +
        data.CSRC_GREAT_CODE_DESC +
        "</td></tr>";
    }
    tableHtml +=
      "<tr><td>所属省/直辖市</td><td>" + data.AREA_NAME + "</td></tr>";
    tableHtml +=
      "<tr><td>状态*</td><td>" +
      (aStatus ? aStatus : "-") +
      "/" +
      (bStatus ? bStatus : "-") +
      "/" +
      (cdrStatus ? cdrStatus : "-") +
      "</td></tr>";
    tableHtml +=
      "<tr><td>是否上证180样本股</td><td>" +
      data.SECURITY_30_DESC +
      "</td></tr>";
    tableHtml +=
      "<tr><td>是否境外上市</td><td>" +
      data.FOREIGN_LISTING_DESC +
      "</td></tr>";
    tableHtml +=
      "<tr><td>境外上市地</td><td>" +
      data.FOREIGN_LISTING_ADDRESS +
      "</td></tr>";
    $(".js_companyOverview table").html(tableHtml);
  },
  // 获取董事会秘书
  getNameOfBS: function () {
    var _this = this;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.nameOfBSParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          _this.nameOfBSData = data.result;
        }
        _this.getCompanyOverviewHtml();
      },
      errCallback: function () {
        _this.getCompanyOverviewHtml();
      },
    });
  },
  // 获取上市日期
  getListingDate: function () {
    var _this = this;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.listingDateParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0) {
          _this.listingDateData = data.result;
        }
        // 获取董事会秘书姓名
        _this.nameOfBSParams.productid = _this.stockCode;
        _this.getNameOfBS();
      },
      errCallback: function () {
        // 获取董事会秘书姓名
        _this.nameOfBSParams.productid = _this.stockCode;
        _this.getNameOfBS();
      },
    });
  },
  // 获取公司概况
  getCompanyOverview: function () {
    var _this = this;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.companyOverviewParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0 && data.result[0]) {
          // 显示公司概况
          $(".js_companyOverview").show();
          // 默认加载第一条数据
          var resultData = data.result[0];
          _this.companyOverviewData = resultData;

          // 变更面包屑内容 股票代码+名称
          $(".js_page-title").html(
            resultData.FULL_NAME + " " + resultData.COMPANY_CODE
          );

          // 利润分配增加历史链接
          $(".js_profit").append(
            '<div class="remarks">' +
            '<a href="http://www.sse.com.cn/assortment/stock/list/info/profit/index.shtml?COMPANY_CODE=' +
            _this.stockCode +
            '" title="2022年1月9日前的历史数据，请点击此处。" target="_blank">2022年1月9日前的历史数据，请点击此处。</a></div>'
          );

          // 成交统计增加历史链接
          $(".js_turnover").append(
            '<div class="remarks">' +
            '<a href="http://www.sse.com.cn/assortment/stock/list/info/turnover/index.shtml?COMPANY_CODE=' +
            _this.stockCode +
            '" title="更多历史数据，请点击此处" target="_blank">更多历史数据，请点击此处</a></div>'
          );

          // 筹资情况增加历史链接
          $(".js_financing").append(
            '<div class="remarks">' +
            '<a href="http://www.sse.com.cn/assortment/stock/list/info/financing/index.shtml?COMPANY_CODE=' +
            _this.stockCode +
            '" title="2022年1月9日前的历史数据，请点击此处。" target="_blank">2022年1月9日前的历史数据，请点击此处。</a></div>'
          );

          // 单位设置
          if (
            resultData.SEC_TYPE != "-" &&
            resultData.SEC_TYPE != "主板A" &&
            resultData.SEC_TYPE != "主板B"
          ) {
            _this.marketIndexParams.select =
              "name,last,chg_rate,change,amount,volume,open,prev_close,ask,bid,high,low,tradephase,turnover_ratio,totalValue,amp_rate,circulating,up_limit,down_limit,fp_volume,fp_amount,cpxxprodusta,cpxxlmttype,fp_phase";
            _this.isStar = true;
            _this.isAStock = true;
            _this.stockUnit = "万股/万份";
            _this.gfUnit = "股/份";
            _this.bshareUnit = "科创板CDR";
            if (resultData.SEC_TYPE == "科创A") {
              _this.ashareUnit = "科创板A股";
            }
            if (resultData.SEC_TYPE == "科创CDR") {
              _this.ashareUnit = "科创板CDR";
            }
            _this.abprofitParams.sqlId = "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_FH_L"; // 利润分配-分红-A股-科创板
            _this.agprofitParams.sqlId = "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_SG_L"; // 利润分配-送股-A股-科创板

            _this.starPublicParams.secCode = _this.stockCode; // 交易信息公开科创板
          } else if (resultData.SEC_TYPE == "主板A") {
            _this.ashareUnit = "A股";
            _this.bshareUnit = "B股";
            _this.isAStock = true;
            _this.marketIndexParams.select =
              "name,last,chg_rate,change,amount,volume,open,prev_close,ask,bid,high,low,tradephase,turnover_ratio,totalValue,amp_rate,circulating,up_limit,down_limit,fp_volume,fp_amount,cpxxprodusta,cpxxlmttype,fp_phase";
            _this.abprofitParams.sqlId = "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_FH_L"; // 利润分配-分红-A股-主板
            _this.agprofitParams.sqlId = "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_SG_L"; // 利润分配-送股-A股-主板

            _this.mainPublicParams.secCode = _this.stockCode; // 交易信息公开主板
            _this.mainPublicParams.tradeDateStart = "2017-01-01";
            _this.mainPublicParams.tradeDateEnd = _this.todayDate;
            $(".js_diclosurePublic .js_dateRange input").val(
              "2017-01-01 - " + _this.todayDate
            );
          } else if (resultData.SEC_TYPE == "主板B") {
            _this.ashareUnit = "A股";
            _this.bshareUnit = "B股";
            _this.isAStock = false;
            _this.marketIndexParams.select =
              "name,last,chg_rate,change,amount,volume,open,prev_close,ask,bid,high,low,tradephase,turnover_ratio,totalValue,amp_rate,circulating,up_limit,down_limit,hlt_tag,gdr_ratio,gdr_prevpx,gdr_currency";
            _this.abprofitParams.sqlId = "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_FH_L"; // 利润分配-分红-B股-主板
            _this.agprofitParams.sqlId = "COMMON_SSE_CP_GPJCTPZ_GPLB_LRFP_SG_L"; // 利润分配-送股-B股-主板

            _this.mainPublicParams.secCode = _this.stockCode; // 交易信息公开主板
            _this.mainPublicParams.tradeDateStart = "2017-01-01";
            _this.mainPublicParams.tradeDateEnd = _this.todayDate;
            $(".js_diclosurePublic .js_dateRange input").val(
              "2017-01-01 - " + _this.todayDate
            );
          }
          // 交易信息公开
          _this.getDiclosurePublicList(1);
          // 行情指标
          _this.getMarketIndex();

          // 主板A
          _this.stockCodeA = resultData.A_STOCK_CODE
            ? resultData.A_STOCK_CODE
            : "";
          if (resultData.A_STOCK_CODE && resultData.A_STOCK_CODE != "-") {
            if (resultData.LIST_BOARD != 1) {
              // 科创板首发/增发
              _this.sfFinancingParams.COMPANY_CODE = _this.stockCode;
              _this.getSfFinancingList();
              // 科创板配股
              _this.srFinancingParams.COMPANY_CODE = _this.stockCode;
              _this.getSrFinancingList();
            } else {
              // A首发/增发
              _this.afFinancingParams.COMPANY_CODE = _this.stockCode;
              _this.getAfFinancingList();
              // A配股
              _this.arFinancingParams.COMPANY_CODE = _this.stockCode;
              _this.getArFinancingList();
            }
          }
          // 主板B
          _this.stockCodeB = resultData.B_STOCK_CODE
            ? resultData.B_STOCK_CODE
            : "";
          if (resultData.B_STOCK_CODE && resultData.B_STOCK_CODE != "-") {
            // B首次发行
            _this.bfFinancingParams.productid = _this.stockCode;
            _this.getBfFinancingList();
            // B增发
            _this.baFinancingParams.productid = _this.stockCode;
            _this.getBaFinancingList();
            // B配股
            _this.brFinancingParams.productid = _this.stockCode;
            _this.getBrFinancingList();
          }

          // 成交统计
          _this.turnoverParams.SEC_CODE = _this.stockCode;
          _this.turnoverParams.TX_DATE_MON = _this.lastTradeDate
            .replace(/\-/g, "")
            .substring(0, 6);
          _this.turnoverParams.TX_DATE_YEAR = _this.lastTradeDate.substring(
            0,
            4
          );
          _this.turnoverParams.TX_DATE = _this.lastTradeDate;
          $(".js_turnover .js_date input").val(_this.lastTradeDate);
          _this.initTurnover();

          // 首日表现
          _this.fristDayParams.COMPANY_CODE = _this.stockCode;
          _this.getFristDayList();
          // 筹资情况 END
          if (resultData.LIST_BOARD && resultData.LIST_BOARD == "2") {
            // 利润分配-送股-科创板
            _this.sgprofitParams.COMPANY_CODE = _this.stockCode;
            _this.getSgProfitList(1);
            // 利润分配-分红-科创板
            _this.sbprofitParams.COMPANY_CODE = _this.stockCode;
            _this.getSbProfitList(1);
          } else {
            // 利润分配-分红-B股
            _this.bbprofitParams.COMPANY_CODE = _this.stockCode;
            _this.getBbProfitList(1);
            // 利润分配-送股-B股
            _this.bgprofitParams.COMPANY_CODE = _this.stockCode;
            //  _this.getBgProfitList(1);

            _this.agprofitParams.COMPANY_CODE = _this.stockCode;
            _this.getAgProfitList(1);
            // 利润分配-分红-A股
            _this.abprofitParams.COMPANY_CODE = _this.stockCode;
            _this.getAbProfitList(1);
          }
          // 利润分配 END

          //调用拼接公司概况的函数
          _this.getCompanyOverviewHtml();

          // 获取股本结构
          _this.capitalstructureParams.COMPANY_CODE = _this.stockCode;
          _this.getCapitalstructure();
          //获取十大股东排名接口
          _this.topTenShareholdersParams.stockId = _this.stockCode;
          _this.getTopTenShareholdersList();
        }
      },
      errCallback: function () { },
    });
  },
  // 获取股票类型
  getStockType1: function () {
    var _this = this;
    getJSONP({
      type: "post",
      dataType: "jsonp",
      url: _this.commonQueryUrl,
      data: _this.stockTypeParams,
      successCallback: function (data) {
        if (data && data.result && data.result.length > 0 && data.result[0]) {
          // 股票类型赋值
          _this.stockType = data.result[0].SUB_TYPE;
          $(".js_calendar").hide(); // 全部隐藏
        } else {
          $(".js_calendar").hide();
          $(".js_calendar .table").html("");
          $(".js_calendar .pagination-box").html("");
        }
      },
      errCallback: function () {
        $(".js_calendar").hide();
        $(".js_calendar .table").html("");
        $(".js_calendar .pagination-box").html("");
      },
    });
  },
};
if ($(".js_company").length > 0) {
  company.init();
}
// 股票概况详情 END

function stringFormatter(d) {
  if (d == null || d == undefined || d == "null" || "" == d || "-" == d) {
    return "-";
  } else {
    if (d.indexOf(".") == -1) {
      return d;
    } else {
      if (d.substring(d.indexOf(".")).length > 1) {
        return parseFloat(d).toFixed(2);
      } else {
        return d;
      }
    }
  }
}

// 取两位小数,最后一位为0时不展示0
function stringFormatterTwo(d) {
  if (d == null || d == undefined || d == "null" || "" == d || "-" == d) {
    return "-";
  } else {
    if (d.indexOf(".") == -1) {
      return d;
    } else {
      if (d.substring(d.indexOf(".")).length > 1) {
        return parseFloat(parseFloat(d).toFixed(2));
      } else {
        return d;
      }
    }
  }
}

function renum(num, s) {
  s = s ? s : 0;
  var result;
  if (num == 0) {
    result = 0;
    return result;
  }
  if (num == "" || num == null || num == undefined || num == "-") {
    result = "-";
    return result;
  }
  if (Math.abs(Number(num)) < 0.005) {
    result = Number(num).toFixed(4);
    return result;
  }
  result = Number(num).toFixed(s);
  return result;
}

/**
 * 获取交易状态描述
 * @param tradephase 交易状态
 * @returns
 */
function getTradephaseText(tradephase) {
  tradephase = tradephase ? tradephase.replace(/\s/gi, "") : "";
  if (!tradephase) return "";
  tradephase = tradephase.substr(0, 1);
  switch (tradephase) {
    case "S":
      tradephase = "开市前";
      break;
    case "C":
      tradephase = "集合竞价";
      break;
    case "D":
      tradephase = "集合竞价结束";
      break;
    case "T":
      tradephase = "连续竞价";
      break;
    case "B":
      tradephase = "午间休市";
      break;
    case "E":
      tradephase = "闭市";
      break;
    case "P":
      tradephase = "停牌";
      break;
    case "M":
      tradephase = "熔断(可恢复)";
      break;
    case "N":
      tradephase = "熔断(至闭市)";
      break;
    case "U":
      tradephase = "收盘集合竞价";
      break;
  }
  return tradephase;
}

/**
 * 交易公开信息根据披露原因封装
 * @param palteType 板块类型 main 主板 star科创板 marginMain融资主板 marginStar 融资科创板
 * @param disclosureType 披露原因类型
 * @param resulteData 接口返回的数据
 * @returns name披露原因名称 hTitle表头名称 hClass表头Class名称 dCode行数据参数信息 infors详情信息
 */
 function dataPackage(palteType, disclosureType, resulteData) {
  var disclosureJson = {};
  // 主板
  if (palteType == "main") {
    //主板交易公开信息
    disclosureJson["11"] = {
      names: [
        ["0", "有价格涨跌幅限制的日收盘价格涨幅偏离值达到7%的前三只证券"],
        ["20211008", "有价格涨跌幅限制的日收盘价格涨幅偏离值达到7%的前五只证券"]
      ],
      name: "有价格涨跌幅限制的日收盘价格涨幅偏离值达到7%的前五（三）只证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "偏离值%",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "5",
      dCode: "main1",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["12"] = {
      names: [
        ["0", "有价格涨跌幅限制的日收盘价格跌幅偏离值达到7%的前三只证券"],
        ["20211008", "有价格涨跌幅限制的日收盘价格跌幅偏离值达到7%的前五只证券"]
      ],
      name: "有价格涨跌幅限制的日收盘价格跌幅偏离值达到7%的前五（三）只证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "偏离值%",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "5",
      dCode: "main1",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["13"] = {
      names: [
        ["0", "有价格涨跌幅限制的日价格振幅达到15%的前三只证券"],
        ["20211008", "有价格涨跌幅限制的日价格振幅达到15%的前五只证券"]
      ],
      name: "有价格涨跌幅限制的日价格振幅达到15%的前（五）三只证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "振幅%",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "5",
      dCode: "main1",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["14"] = {
      names: [
        ["0", "有价格涨跌幅限制的日换手率达到20%的前三只证券"],
        ["20211008", "有价格涨跌幅限制的日换手率达到20%的前五只证券"]
      ],
      name: "有价格涨跌幅限制的日换手率达到20%的前（五）三只证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "换手率%",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "5",
      dCode: "main1",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["15"] = {
      names: [
        ["0", "无价格涨跌幅限制的证券"],
        ["20230410", "无价格涨跌幅限制首个交易日的证券"]
      ],
      name: "无价格涨跌幅限制（首个交易日）的证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "6",
      dCode: "main4",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["1"] = {
      names: [
        ["0", "非ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到20%的证券"],
        ["20260706", "非S证券连续三个交易日内收盘价格涨幅偏离值累计达到20%的证券"]
      ],
      name: "非S（ST、*ST）证券连续三个交易日内收盘价格涨幅偏离值累计达到20%的证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "累计偏离值%",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "7",
      dCode: "main3",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["2"] = {
      names: [
        ["0", "非ST、*ST和S证券连续三个交易日内收盘价格跌幅偏离值累计达到20%的证券"],
        ["20260706", "非S证券连续三个交易日内收盘价格跌幅偏离值累计达到20%的证券"]
      ],
      name: "非S（ST、*ST）证券连续三个交易日内收盘价格跌幅偏离值累计达到20%的证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "累计偏离值%",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "7",
      dCode: "main3",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["7"] = {
      names: [
        ["20230410", "S股连续三个交易日内收盘价格涨幅偏离值累计达到15%的证券"]
      ],
      name: "S股连续三个交易日内收盘价格涨幅偏离值累计达到15%的证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "累计偏离值%",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "7",
      dCode: "main3",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["8"] = {
      names: [
        ["20230410", "S股连续三个交易日内收盘价格跌幅偏离值累计达到15%的证券"]
      ],
      name: "S股连续三个交易日内收盘价格跌幅偏离值累计达到15%的证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "累计偏离值%",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "7",
      dCode: "main3",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["3"] = {
      names: [
        ["0", "ST、*ST和S证券连续三个交易日内收盘价格涨幅偏离值累计达到15%的证券"],
        ["20230410", "ST、*ST证券连续三个交易日内收盘价格涨幅偏离值累计达到12%的证券"]
      ],
      name: "ST、*ST（和S）证券连续三个交易日内收盘价格涨幅偏离值累计达到15%（12%）的证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "累计偏离值%",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "7",
      dCode: "main3",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["4"] = {
      names: [
        ["0", "ST、*ST和S证券连续三个交易日内收盘价格跌幅偏离值累计达到15%的证券"],
        ["20230410", "ST、*ST证券连续三个交易日内收盘价格跌幅偏离值累计达到12%的证券"]
      ],
      name: "ST、*ST（和S）证券连续三个交易日内收盘价格跌幅偏离值累计达到15%（12%）的证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "累计偏离值%",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "7",
      dCode: "main3",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["5"] = {
      names: [
        ["0", "连续三个交易日内的日均换手率与前五个交易日日均换手率的比值到达30倍，并且该股票、封闭式基金连续三个交易日内累计换手率达到20%"]
      ],
      name: "连续三个交易日内的日均换手率与前五个交易日日均换手率的比值到达30倍，并且该股票、封闭式基金连续三个交易日内累计换手率达到20%",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "换手率%",
        "成交量（万股/万份）",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "5",
      dCode: "main1",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["31"] = {
      names: [
        ["0", "当日无价格涨跌幅限制的A股，出现异常波动停牌的"]
      ],
      name: "当日无价格涨跌幅限制的A股，出现异常波动停牌的",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "6",
      dCode: "main4",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["32"] = {
      names: [
        ["0", "当日无价格涨跌幅限制的B股，出现异常波动停牌的"]
      ],
      name: "当日无价格涨跌幅限制的B股，出现异常波动停牌的",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "6",
      dCode: "main4",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["41"] = {
      names: [
        ["0", "单只标的证券的当日融资买入数量达到当日该证券总交易量的50％以上"]
      ],
      name: "单只标的证券的当日融资买入数量达到当日该证券总交易量的50％以上",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "成交占比%",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "5",
      dCode: "main1",
      infors: "mainmember",
      data: [],
    };
    disclosureJson["42"] = {
      names: [
        ["0", "单只标的证券的当日融券卖出数量达到当日该证券总交易量的50％以上"]
      ],
      name: "单只标的证券的当日融券卖出数量达到当日该证券总交易量的50％以上",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "成交占比%",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "5",
      dCode: "main1",
      infors: "mainmember",
      data: [],
    };
    disclosureJson["37"] = {
      names: [
        ["0", "风险警示股票盘中换手率达到或超过30"]
      ],
      name: "风险警示股票盘中换手率达到或超过30",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "换手率%",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "5",
      dCode: "main1",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["51"] = {
      names: [
        ["0", "退市整理的证券"]
      ],
      name: "退市整理的证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "6",
      dCode: "main4",
      infors: "mainsales",
      data: [],
    };
    disclosureJson["52"] = {
      names: [
        ["0", "实施特别停牌的证券"]
      ],
      name: "实施特别停牌的证券",
      hTitle: [
        "代码",
        "简称",
        "证券种类",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "7",
      dCode: "main4",
      infors: "mainsales",
      data: [],
    };
    //主板严重异常波动
    disclosureJson["Z3"] = {
      names: [
        ["0", "连续10个交易日内4次出现同正向异常波动的证券"]
      ],
      name: "连续10个交易日内4次出现同正向异常波动的证券",
      hTitle: [
        "代码",
        "简称",
        "累计成交量（万股/万份）",
        "累计成交金额（万元）",
        "异常期间",
        "详情",
      ],
      hClass: "4",
      dCode: "main6",
      infors: "investor",
      data: [],
    };
    disclosureJson["Z4"] = {
      names: [
        ["0", "连续10个交易日内4次出现同负向异常波动的证券"]
      ],
      name: "连续10个交易日内4次出现同负向异常波动的证券",
      hTitle: [
        "代码",
        "简称",
        "累计成交量（万股/万份）",
        "累计成交金额（万元）",
        "异常期间",
        "详情",
      ],
      hClass: "4",
      dCode: "main6",
      infors: "investor",
      data: [],
    };
    disclosureJson["Z5"] = {
      names: [
        ["0", "有价格涨跌幅限制的连续10个交易日内收盘价格涨幅偏离值累计达到100%的证券"]
      ],
      name: "有价格涨跌幅限制的连续10个交易日内收盘价格涨幅偏离值累计达到100%的证券",
      hTitle: [
        "代码",
        "简称",
        "累计偏离值（%）",
        "累计成交量（万股/万份）",
        "累计成交金额（万元）",
        "异常期间",
        "详情",
      ],
      hClass: "7",
      dCode: "main7",
      infors: "investor",
      data: [],
    };
    disclosureJson["Z6"] = {
      names: [
        ["0", "有价格涨跌幅限制的连续10个交易日内收盘价格跌幅偏离值累计达到-50%的证券"]
      ],
      name: "有价格涨跌幅限制的连续10个交易日内收盘价格跌幅偏离值累计达到-50%的证券",
      hTitle: [
        "代码",
        "简称",
        "累计偏离值（%）",
        "累计成交量（万股/万份）",
        "累计成交金额（万元）",
        "异常期间",
        "详情",
      ],
      hClass: "7",
      dCode: "main7",
      infors: "investor",
      data: [],
    };
    disclosureJson["Z7"] = {
      names: [
        ["0", "有价格涨跌幅限制的连续30个交易日内收盘价格涨幅偏离值累计达到200%的证券"]
      ],
      name: "有价格涨跌幅限制的连续30个交易日内收盘价格涨幅偏离值累计达到200%的证券",
      hTitle: [
        "代码",
        "简称",
        "累计偏离值（%）",
        "累计成交量（万股/万份）",
        "累计成交金额（万元）",
        "异常期间",
        "详情",
      ],
      hClass: "7",
      dCode: "main7",
      infors: "investor",
      data: [],
    };
    disclosureJson["Z8"] = {
      names: [
        ["0", "有价格涨跌幅限制的连续30个交易日内收盘价格跌幅偏离值累计达到-70%的证券"]
      ],
      name: "有价格涨跌幅限制的连续30个交易日内收盘价格跌幅偏离值累计达到-70%的证券",
      hTitle: [
        "代码",
        "简称",
        "累计偏离值(%)",
        "累计成交量（万股/万份）",
        "累计成交金额（万元）",
        "异常期间",
        "详情",
      ],
      hClass: "7",
      dCode: "main7",
      infors: "investor",
      data: [],
    };
  }
  // 科创板
  if (palteType == "star") {
    //科创板交易公开信息
    disclosureJson["1"] = {
      names: [
        ["0", "有价格涨跌幅限制的日收盘价格涨幅达到15%的前五只证券"]
      ],
      name: "有价格涨跌幅限制的日收盘价格涨幅达到15%的前五只证券",
      hTitle: [
        "代码",
        "简称",
        "涨幅(%)",
        "成交量（万股/万份）",
        "成交金额（万元）",
        "详情",
      ],
      hClass: "1",
      dCode: "star1",
      infors: "starsales",
      data: [],
    };
    disclosureJson["2"] = {
      names: [
        ["0", "有价格涨跌幅限制的日收盘价格跌幅达到15%的前五只证券"]
      ],
      name: "有价格涨跌幅限制的日收盘价格跌幅达到15%的前五只证券",
      hTitle: [
        "代码",
        "简称",
        "跌幅(%)",
        "成交量（万股/万份）",
        "成交金额（万元）",
        "详情",
      ],
      hClass: "1",
      dCode: "star1",
      infors: "starsales",
      data: [],
    };
    disclosureJson["3"] = {
      names: [
        ["0", "有价格涨跌幅限制的日价格振幅达到30%的前五只证券"]
      ],
      name: "有价格涨跌幅限制的日价格振幅达到30%的前五只证券",
      hTitle: [
        "代码",
        "简称",
        "振幅(%)",
        "成交量（万股/万份）",
        "成交金额（万元）",
        "详情",
      ],
      hClass: "1",
      dCode: "star1",
      infors: "starsales",
      data: [],
    };
    disclosureJson["4"] = {
      names: [
        ["0", "有价格涨跌幅限制的日换手率达到30%的前五只证券"]
      ],
      name: "有价格涨跌幅限制的日换手率达到30%的前五只证券",
      hTitle: [
        "代码",
        "简称",
        "换手率(%)",
        "成交量（万股/万份）",
        "成交金额（万元）",
        "详情",
      ],
      hClass: "1",
      dCode: "star1",
      infors: "starsales",
      data: [],
    };
    disclosureJson["5"] = {
      names: [
        ["0", "有价格涨跌幅限制的连续3个交易日内收盘价格涨幅偏离值累计达到30%的证券"]
      ],
      name: "有价格涨跌幅限制的连续3个交易日内收盘价格涨幅偏离值累计达到30%的证券",
      hTitle: [
        "代码",
        "简称",
        "累计偏离值(%)",
        "累计成交量（万股/万份）",
        "累计成交金额（万元）",
        "异常期间",
        "详情",
      ],
      hClass: "3",
      dCode: "star3",
      infors: "starsales",
      data: [],
    };
    disclosureJson["6"] = {
      names: [
        ["0", "有价格涨跌幅限制的连续3个交易日内收盘价格跌幅偏离值累计达到30%的证券"]
      ],
      name: "有价格涨跌幅限制的连续3个交易日内收盘价格跌幅偏离值累计达到30%的证券",
      hTitle: [
        "代码",
        "简称",
        "累计偏离值(%)",
        "累计成交量（万股/万份）",
        "累计成交金额（万元）",
        "异常期间",
        "详情",
      ],
      hClass: "3",
      dCode: "star3",
      infors: "starsales",
      data: [],
    };
    //科创板严重异常波动
    disclosureJson["7"] = {
      names: [
        ["0", "连续10个交易日内3次出现同正向异常波动的证券"]
      ],
      name: "连续10个交易日内3次出现同正向异常波动的证券",
      hTitle: [
        "代码",
        "简称",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "4",
      dCode: "star4",
      infors: "investor",
      data: [],
    };
    disclosureJson["8"] = {
      names: [
        ["0", "连续10个交易日内3次出现同负向异常波动的证券"]
      ],
      name: "连续10个交易日内3次出现同负向异常波动的证券",
      hTitle: [
        "代码",
        "简称",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "4",
      dCode: "star4",
      infors: "investor",
      data: [],
    };
    disclosureJson["9"] = {
      names: [
        ["0", "有价格涨跌幅限制的连续10个交易日内收盘价格涨幅偏离值累计达到100%的证券"]
      ],
      name: "有价格涨跌幅限制的连续10个交易日内收盘价格涨幅偏离值累计达到100%的证券",
      hTitle: [
        "代码",
        "简称",
        "累计偏离值(%)",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "3",
      dCode: "star3",
      infors: "investor",
      data: [],
    };
    disclosureJson["10"] = {
      names: [
        ["0", "有价格涨跌幅限制的连续10个交易日内收盘价格跌幅偏离值累计达到-50%的证券"]
      ],
      name: "有价格涨跌幅限制的连续10个交易日内收盘价格跌幅偏离值累计达到-50%的证券",
      hTitle: [
        "代码",
        "简称",
        "累计偏离值(%)",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "3",
      dCode: "star3",
      infors: "investor",
      data: [],
    };
    disclosureJson["11"] = {
      names: [
        ["0", "有价格涨跌幅限制的连续30个交易日内收盘价格涨幅偏离值累计达到200%的证券"]
      ],
      name: "有价格涨跌幅限制的连续30个交易日内收盘价格涨幅偏离值累计达到200%的证券",
      hTitle: [
        "代码",
        "简称",
        "累计偏离值(%)",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "3",
      dCode: "star3",
      infors: "investor",
      data: [],
    };
    disclosureJson["12"] = {
      names: [
        ["0", "有价格涨跌幅限制的连续30个交易日内收盘价格跌幅偏离值累计达到-70%的证券"]
      ],
      name: "有价格涨跌幅限制的连续30个交易日内收盘价格跌幅偏离值累计达到-70%的证券",
      hTitle: [
        "代码",
        "简称",
        "累计偏离值(%)",
        "累计成交量(万股/万份)",
        "累计成交金额(万元)",
        "异常期间",
        "详情",
      ],
      hClass: "3",
      dCode: "star3",
      infors: "investor",
      data: [],
    };
    //融资融券超50%
    disclosureJson["13"] = {
      names: [
        ["0", "单只标的证券的当日融资买入数量达到当日该证券总交易量的50％以上"]
      ],
      name: "单只标的证券的当日融资买入数量达到当日该证券总交易量的50％以上",
      hTitle: [
        "代码",
        "简称",
        "成交占比(%)",
        "成交量(万股/万份)",
        "成交金额（万元）",
        "详情",
      ],
      hClass: "1",
      dCode: "star3",
      infors: "starsales",
      data: [],
    };
    disclosureJson["14"] = {
      names: [
        ["0", "单只标的证券的当日融券卖出数量达到当日该证券总交易量的50％以上"]
      ],
      name: "单只标的证券的当日融券卖出数量达到当日该证券总交易量的50％以上",
      hTitle: [
        "代码",
        "简称",
        "成交占比(%)",
        "成交量(万股/万份)",
        "成交金额（万元）",
        "详情",
      ],
      hClass: "1",
      dCode: "star3",
      infors: "starsales",
      data: [],
    };
    disclosureJson["15"] = {
      names: [
        ["0", "实施特别停牌的证券"]
      ],
      name: "实施特别停牌的证券",
      hTitle: ["代码", "简称", "成交量(万股/万份)", "成交金额（万元）", "详情"],
      hClass: "2",
      dCode: "star2",
      infors: "starsales",
      data: [],
    };
    disclosureJson["16"] = {
      names: [
        ["0", "退市整理的证券"]
      ],
      name: "退市整理的证券",
      hTitle: [
        "代码",
        "简称",
        "成交量(万股/万份)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "2",
      dCode: "star2",
      infors: "starsales",
      data: [],
    };
  }
  // 融资主板
  if (palteType == "marginMain") {
    disclosureJson["41"] = {
      name: "一、单只标的证券的当日融资买入数量达到当日该证券总交易量的50％以上",
      hTitle: [
        "代码",
        "简称",
        "成交占比%",
        "成交量(万股)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "1",
      dCode: "main5",
      infors: "mainmember",
      data: [],
    };
    disclosureJson["42"] = {
      name: "二、单只标的证券的当日融券卖出数量达到当日该证券总交易量的50％以上",
      hTitle: [
        "代码",
        "简称",
        "成交占比%",
        "成交量(万股)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "1",
      dCode: "main5",
      infors: "mainmember",
      data: [],
    };
  }
  //融资主板
  if (palteType == "marginMain") {
    disclosureJson["41"] = {
      names: [
        ["0", "单只标的证券的当日融资买入数量达到当日该证券总交易量的50％以上"]
      ],
      name: "单只标的证券的当日融资买入数量达到当日该证券总交易量的50％以上",
      hTitle: [
        "代码",
        "简称",
        "成交占比%",
        "成交量(万股)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "1",
      dCode: "main5",
      infors: "mainmember",
      data: [],
    };
    disclosureJson["42"] = {
      names: [
        ["0", "单只标的证券的当日融券卖出数量达到当日该证券总交易量的50％以上"]
      ],
      name: "单只标的证券的当日融券卖出数量达到当日该证券总交易量的50％以上",
      hTitle: [
        "代码",
        "简称",
        "成交占比%",
        "成交量(万股)",
        "成交金额(万元)",
        "详情",
      ],
      hClass: "1",
      dCode: "main5",
      infors: "mainmember",
      data: [],
    };
  }
  // 融资科创板
  if (palteType == "marginStar") {
    disclosureJson["13"] = {
      names: [
        ["0", "单只标的证券的当日融资买入数量达到当日该证券总交易量的50％以上"]
      ],
      name: "单只标的证券的当日融资买入数量达到当日该证券总交易量的50％以上",
      hTitle: [
        "代码",
        "简称",
        "成交占比%",
        "成交量（万股/万份）",
        "成交金额（万元）",
        "详情",
      ],
      hClass: "1",
      dCode: "star1",
      infors: "starsales",
      data: [],
    };
    disclosureJson["14"] = {
      names: [
        ["0", "单只标的证券的当日融券卖出数量达到当日该证券总交易量的50％以上"]
      ],
      name: "单只标的证券的当日融券卖出数量达到当日该证券总交易量的50％以上",
      hTitle: [
        "代码",
        "简称",
        "成交占比%",
        "成交量（万股/万份）",
        "成交金额（万元）",
        "详情",
      ],
      hClass: "1",
      dCode: "star1",
      infors: "starsales",
      data: [],
    };
  }
  // 数据封装
  if (resulteData && resulteData.length > 0) {
    $.each(resulteData, function (k, v) {
      // 主板接口的披露原因为空时，数据拼接到disclosureJson['52']中
      if (disclosureJson[v.refType]) {
        disclosureJson[v.refType].data.push(v);
      }
    });
  }
  // 披露原因类型时返回具体披露信息
  if (disclosureType) return disclosureJson[disclosureType];
  return disclosureJson;
}

