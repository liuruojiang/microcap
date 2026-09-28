/**
 * 行情信息-行情报表
 */
var report = {
  dateUrl: "equity", //接口地址 equity 股票-全部 ashare，股票-主板A股 ashare，股票-主板B股 bshare，股票-科创板 kshare，基金 fund，债券 bond，指数 index
  select:
    "code,name,open,high,low,last,prev_close,chg_rate,volume,amount,tradephase,change,amp_rate,cpxxsubtype,cpxxprodusta,", //接口查询select
  order: "", //接口排序方式
  sortName: "", //排序名称
  sortOrder: "", //排序方式
  pageSize: 25, //每页条数
  reportTypeOption: [
    { value: "1", name: "股票" },
    { value: "2", name: "基金" },
    { value: "3", name: "债券" },
    { value: "4", name: "指数" },
    { value: "5", name: "期权" },
    { value: "6", name: "公募REITs" },
  ],
  shareTypeOption: [
    { value: "1", name: "全部板块" },
    { value: "2", name: "主板A股" },
    { value: "3", name: "主板B股" },
    { value: "4", name: "科创板" },
  ],
  init: function () {
    this.loadEvents();
  },
  loadEvents: function () {
    var _this = this;
    $(".js_report .title_lev2 a")
      .attr("class", "tableDownload")
      .attr("href", "javascript:;")
      .html("刷新");
    //下拉框数据渲染
    bootstrapSelect({
      method: function () {
        //类型数据渲染
        $(".js_reportType .selectpicker")
          .html(addSelectOption(_this.reportTypeOption))
          .selectpicker("refresh")
          .selectpicker("render");
        //下拉框改变触发查询
        $(".js_reportType .selectpicker").on("changed.bs.select", function (e) {
          _this.setReportParams();
        });
        $(".js_shareType .selectpicker")
          .html(addSelectOption(_this.shareTypeOption))
          .selectpicker("refresh")
          .selectpicker("render");
        //下拉框改变触发查询
        $(".js_shareType .selectpicker").on("changed.bs.select", function (e) {
          _this.setReportParams();
        });
      },
    });
    //表头排序点击事件
    $(".js_report").on("click", ".js_orderBtn", function () {
      var dataOrder = $(this).attr("data-order").split(",");
      _this.sortName = dataOrder[0];
      if (!dataOrder[1] || dataOrder[1] == "asc") {
        _this.sortOrder = "desc";
        _this.order = dataOrder[0]; //正序 比如name,ase 倒序 name
      } else {
        _this.sortOrder = "asc";
        _this.order = dataOrder[0] + ",ase"; //正序 比如name,ase 倒序 name
      }
      _this.setReportParams();
    });
    $(".js_report").on("click", ".title_lev2 a", function () {
      _this.setReportParams();
    });
    _this.getReportList(1);
  },
  setReportParams: function () {
    //参数处理
    var _this = report;
    var reportType = $(".js_reportType .selectpicker").val()
      ? $(".js_reportType .selectpicker").val()
      : "1"; //指数类型
    var shareType = $(".js_shareType .selectpicker").val()
      ? $(".js_shareType .selectpicker").val()
      : "1"; //股票类型
    $(".js_shareType").hide(); //默认隐藏股票类型
    if (reportType == "5") window.location.href = "/assortment/options/price/"; //期权跳转
    if (reportType == "1") $(".js_shareType").show(); //股票显示股票类型

    //股票 select
    if (reportType == "1")
      _this.select =
        "code,name,open,high,low,last,prev_close,chg_rate,volume,amount,tradephase,change,amp_rate,cpxxsubtype,cpxxprodusta";
    //基金 公募reits select
    if (reportType == "2" || reportType == "6")
      _this.select =
        "code,cpxxextendname,open,high,low,last,prev_close,chg_rate,volume,amount,tradephase,change,amp_rate,cpxxsubtype";
    //债券 指数 select
    if (reportType == "3" || reportType == "4")
      _this.select =
        "code,name,open,high,low,last,prev_close,chg_rate,volume,amount,tradephase,change,amp_rate,cpxxsubtype";
    //股票-全部 dateUrl
    if (reportType == "1" && shareType == "1") _this.dateUrl = "equity";
    //股票-主板A股 dateUrl
    if (reportType == "1" && shareType == "2") _this.dateUrl = "ashare";
    //股票-主板B股 dateUrl
    if (reportType == "1" && shareType == "3") _this.dateUrl = "bshare";
    //股票-科创板 dateUrl
    if (reportType == "1" && shareType == "4") _this.dateUrl = "kshare";
    //基金 dateUrl
    if (reportType == "2") _this.dateUrl = "fund";
    //债券 dateUrl
    if (reportType == "3") _this.dateUrl = isShb1;
    //指数 dateUrl
    if (reportType == "4") _this.dateUrl = "index";
    //公募REITs dateUrl
    if (reportType == "6") _this.dateUrl = "reits";
    _this.getReportList(1);
  },
  getReportList: function (pageIndex) {
    var _this = this;
    var reportType = $(".js_reportType .selectpicker").val()
      ? $(".js_reportType .selectpicker").val()
      : "1"; //指数类型
    var shareType = $(".js_shareType .selectpicker").val()
      ? $(".js_shareType .selectpicker").val()
      : "1"; //股票类型
    //if (!paginationChange(_this.reportParams, pageIndex)) return;//触发分页改变分页参数
    var emptyTr = '<tr><td colspan="3">暂无数据</td></tr>';
    var reportHtml = "",
      periodicHtml = "";
    //获取表头
    reportHtml += _this.getTbaleHearderHtml(reportType);
    reportHtml += "<tbody>";
    //页数处理
    pageIndex = dynaGetData(null, pageIndex);
    if (isNaN(pageIndex)) {
      return false;
    }
    getJSONP({
      type: "post",
      dataType: "jsonp",
      // url: hq_queryUrl + "/v1/sh1/list/exchange/" + _this.dateUrl,
      url:
        hq_queryUrl +
        (reportType == "3" ? hqBondUrl : hqOthUrl) +
        "list/exchange/" +
        _this.dateUrl,
      data: {
        select: _this.select,
        order: _this.order,
        begin: (pageIndex - 1) * _this.pageSize,
        end: pageIndex * _this.pageSize,
      },
      jsonp: "callback",
      successCallback: function (data) {
        if (
          !data ||
          (data && !data.list) ||
          (data && data.list && data.list.length == 0)
        ) {
          //接口放回为空
          periodicHtml += emptyTr;
          periodicHtml += "</tbody>";
          $(".js_report .table").html(periodicHtml);
          $(".js_report .pagination-box").html("");
          return false;
        }
        //获取表内容样式 reportType-指数类型  shareType-股票类型 data-接口返回数据 reportHtml-html样式 pageIndex-页数
        reportHtml += _this.getTbaleDateHtml(
          reportType,
          shareType,
          data,
          pageIndex
        );
        reportHtml += "</tbody>";
        $(".js_report .table").html(reportHtml);
        //调用分页 总页数 总条数 当前页 每页条数
        Page.navigation(
          ".js_report .pagination-box",
          Math.ceil(data.total / _this.pageSize),
          data.total,
          pageIndex,
          _this.pageSize,
          "report.getReportList"
        );
        $(".js_report .pagination-box .change-pageSize").remove();
      },
      errCallback: function () {
        periodicHtml += emptyTr;
        periodicHtml += "</tbody>";
        $(".js_report .table").html(periodicHtml);
        $(".js_report .pagination-box").html("");
      },
    });
  },
  getTbaleDateHtml: function (reportType, shareType, resultData, pageIndex) {
    //处理表内容html
    var _this = this;
    var dateHtml = "";
    var _nowTime =
      ("" + resultData.time).length == 5
        ? "0" + resultData.time
        : "" + resultData.time;
    var _sysDateTime = resultData.date + "" + _nowTime;
    _sysDateTime =
      _sysDateTime.substring(0, 4) +
      "-" +
      _sysDateTime.substring(4, 6) +
      "-" +
      _sysDateTime.substring(6, 8) +
      " " +
      _sysDateTime.substring(8, 10) +
      ":" +
      _sysDateTime.substring(10, 12) +
      ":" +
      _sysDateTime.substring(12);
    $(".js_report .new_date").html("更新时间：" + _sysDateTime);
    $.each(resultData.list, function (k, v) {
      if (v[10].replace(/\s/gi, "") == "C1") {
        $(".js_report .new_date").html("集合竞价中 更新时间：" + _sysDateTime);
      }
      //href 跳转地址处理
      var link = "",
        _lastColor,
        _last, //最新
        _chg_rateColor,
        _chg_rate, //涨跌幅
        _changeColor,
        _change, //涨跌
        _volumeColor,
        _volume, //成交量(手)
        _amountColor,
        _amount, //成交额
        _prev_closeColor,
        _prev_close, ///前收
        _openColor,
        _open, //开盘
        _highColor,
        _high, //最高
        _lowColor,
        _low; //最低
      if (reportType == "1")
        link =
          "/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=" +
          v[0]; //股票
      if (reportType == "6")
        link = '/reits/assortment/prices/detail/price/?code=' + v[0] + '&name=' + v[1]; //基金RET
      if (reportType == "2" && v[13] == "EBS") //ETF
        link = "/assortment/fund/list/etfinfo/basic/index.shtml?FUNDID=" + v[0];
      if (reportType == "2" && v[13] == "LOF") //LOF
        link = "/assortment/fund/list/lofinfo/basic/index.shtml?FUNDID=" + v[0];
      if (reportType == "3")
        link = sseUtil.BondCodeSearthUrl(v[0])
          ? sseUtil.BondCodeSearthUrl(v[0]) + "?BOND_CODE=" + v[0] + "&TYPE=y&BOND_TYPE=全部"
          : ""; //债券
      if (reportType == "4")
        link =""; //指数
      //涨幅颜色处理及各数据处理
      _lastColor = Comparative(v[5], v[6]); //最新
      // _last = v[5].toFixed(2); //最新
      _chg_rateColor = Comparative(v[7], 0); //涨跌幅
      _chg_rate = v[7].toFixed(2); //涨跌幅
      _changeColor = Comparative(v[7], 0); //涨跌
      // _change = v[11].toFixed(2); //涨跌
      _volume = (v[8] / 100).toFixed(0); //成交量(手)
      _amount = (v[9] / 10000).toFixed(2); //成交额
      // _prev_close = v[6].toFixed(2); ///前收
      _openColor = Comparative(v[2], v[6]); //开盘
      // _open = v[2].toFixed(2); //开盘
      _highColor = Comparative(v[3], v[6]); //最高
      // _high = v[3].toFixed(2); //最高
      _lowColor = Comparative(v[4], v[6]); //最低
      // _low = v[4].toFixed(2); //最低
      // 债券（最新、涨跌、前收、开盘、最高、最低）限制三位小数位
      if (reportType == "3") {
        _last = v[5].toFixed(3); //最新
        _change = v[11].toFixed(3); //涨跌
        _prev_close = v[6].toFixed(3); ///前收
        _open = v[2].toFixed(3); //开盘
        _high = v[3].toFixed(3); //最高
        _low = v[4].toFixed(3); //最低
      } else {
        _last = v[5].toFixed(2); //最新
        _change = v[11].toFixed(2); //涨跌
        _prev_close = v[6].toFixed(2); ///前收
        _open = v[2].toFixed(2); //开盘
        _high = v[3].toFixed(2); //最高
        _low = v[4].toFixed(2); //最低
      }
      if (
        v[10].replace(/\s/gi, "") == "P1" ||
        ((reportType == "1" || reportType == "2" || reportType == "3" || reportType == "6") &&
          v[5].toFixed(2) == 0)
      ) {
        //停牌 或者 股票/基金/债券最新为0
        _lastColor = ""; //最新
        if (v[10].replace(/\s/gi, "") == "P1") _last = "停牌"; //停牌
        if (v[10].replace(/\s/gi, "") != "P1") _last = "暂无成交"; //股票/基金最新为0
        _chg_rateColor = ""; //涨跌幅
        _chg_rate = "--"; //涨跌幅
        _changeColor = ""; //涨跌
        _change = "--"; //涨跌
        _volume = "--"; //成交量(手)
        _amount = "--"; //成交额
        _prev_close = v[6].toFixed(2); ///前收
        _openColor = ""; //开盘
        _open = "--"; //开盘
        _highColor = ""; //最高
        _high = "--"; //最高
        _lowColor = ""; //最低
        _low = "--"; //最低
      } else if ((reportType == "2" || reportType == "6") && v[5].toFixed(2) != 0) {
        //基金 最新不为0
        _last = v[5].toFixed(3); //最新
        _change = v[11].toFixed(3); //涨跌
        _prev_close = v[6].toFixed(3); //前收
        _open = v[2].toFixed(3); //开盘
        _high = v[3].toFixed(3); //最高
        _low = v[4].toFixed(3); //最低
      } else if (reportType == "3") {
        //债券
        _volume = v[8]; //成交量(手)
        _amount = (v[9] / 10000).toFixed(2); //成交额(万元)
      } else if (reportType == "4") {
        //指数
        _volume = v[8]; //成交量(手)
        _amount = (v[9] / 100000000).toFixed(2); //成交额(万元)
      }
      dateHtml += "<tr>";
      dateHtml += "<td>" + ((pageIndex - 1) * _this.pageSize + k + 1) + "</td>"; //序号
      dateHtml +=
        '<td class="codeNameWidth">' +
        (link ? '<a href="' + link + '" target="_blank">' : "") +
        v[0] +
        (link ? "</a>" : "") +
        "</td>"; //证券代码
      if (reportType == "1") { // 主板、科创板简称后加标识
        dateHtml += '<td class="text-nowrap"><span>' + v[1] + kcbShowIconFlag(v[14]) + "</span></td>"; //证券简称
      } else {
        dateHtml += '<td class="text-nowrap"><span>' + v[1] + "</span></td>"; //证券简称
      }
      if (reportType == "1" && v[13] == "KSH")
        dateHtml += '<td class="text-nowrap">科创板</td>'; //类型
      if (reportType == "1" && v[13] == "ASH")
        dateHtml += '<td class="text-nowrap">主板A股</td>'; //类型
      if (reportType == "1" && v[13] == "BSH")
        dateHtml += '<td class="text-nowrap">主板B股</td>'; //类型
      dateHtml +=
        '<td class="text-right ' + _lastColor + '">' + _last + "</td>"; //最新
      dateHtml +=
        '<td class="text-right ' +
        _chg_rateColor +
        '">' +
        _chg_rate +
        (_chg_rate != "--" ? "%" : "") +
        "</td>"; //涨跌幅
      dateHtml +=
        '<td class="text-right ' + _changeColor + '">' + _change + "</td>"; //涨跌
      dateHtml += '<td class="text-right">' + _volume + "</td>"; //成交量(手)
      dateHtml += '<td class="text-right">' + _amount + "</td>"; //成交额(万元)
      dateHtml += '<td class="text-right">' + _prev_close + "</td>"; //前收
      dateHtml +=
        '<td class="text-right ' + _openColor + '">' + _open + "</td>"; //开盘\
      dateHtml +=
        '<td class="text-right ' + _highColor + '">' + _high + "</td>"; //最高
      dateHtml += '<td class="text-right ' + _lowColor + '">' + _low + "</td>"; //最低
    });
    return dateHtml;
  },
  getTbaleHearderHtml: function (reportType) {
    //设置表头
    var _this = this;
    // sortName-证券代码:code,证券简称：name,最新:last,涨跌幅:chg_rate,涨跌:change,成交量:volume,成交额:amount,前收:prev_close,开盘:open,最高:high,最低:low
    // sortOrder-倒序：desc,正序：asc
    var name = _this.sortName,
      order = _this.sortOrder;
    hearderHtml = "<thead><tr>";
    hearderHtml += "<th>序号</th>";
    if (reportType == "6") {
      hearderHtml +=
      '<th class="js_orderBtn text-nowrap" data-order="code' +
      (order ? "," + order : "") +
      '">代码<span class="sort_btn"><em class="caret_up  ' +
      (name == "code" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "code" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    } else {
      hearderHtml +=
      '<th class="js_orderBtn text-nowrap" data-order="code' +
      (order ? "," + order : "") +
      '">证券代码<span class="sort_btn"><em class="caret_up  ' +
      (name == "code" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "code" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    }
    if (reportType == "2") {
      hearderHtml +=
        '<th class="js_orderBtn text-nowrap" data-order="name' +
        (order ? "," + order : "") +
        '">基金扩位简称<span class="sort_btn"><em class="caret_up  ' +
        (name == "name" && order == "asc" ? "fill" : "") +
        '"></em><em class="caret_down ' +
        (name == "name" && order == "desc" ? "fill" : "") +
        '"></em></span></th>';
    } else if (reportType == "6") {
      hearderHtml +=
      '<th class="js_orderBtn text-nowrap" data-order="name' +
      (order ? "," + order : "") +
      '">扩位简称<span class="sort_btn"><em class="caret_up  ' +
      (name == "name" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "name" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    } else {
      hearderHtml +=
        '<th class="js_orderBtn text-nowrap" data-order="name' +
        (order ? "," + order : "") +
        '">证券简称<span class="sort_btn"><em class="caret_up  ' +
        (name == "name" && order == "asc" ? "fill" : "") +
        '"></em><em class="caret_down ' +
        (name == "name" && order == "desc" ? "fill" : "") +
        '"></em></span></th>';
    }
    if (reportType == "1") hearderHtml += '<th class="text-nowrap">类型</th>'; //股票显示

    hearderHtml +=
      '<th class="js_orderBtn text-right" data-order="last' +
      (order ? "," + order : "") +
      '">最新<span class="sort_btn"><em class="caret_up  ' +
      (name == "last" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "last" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    hearderHtml +=
      '<th class="js_orderBtn text-right" data-order="chg_rate' +
      (order ? "," + order : "") +
      '">涨跌幅<span class="sort_btn"><em class="caret_up  ' +
      (name == "chg_rate" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "chg_rate" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    hearderHtml +=
      '<th class="js_orderBtn text-right" data-order="change' +
      (order ? "," + order : "") +
      '">涨跌<span class="sort_btn"><em class="caret_up  ' +
      (name == "change" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "change" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    hearderHtml +=
      '<th class="js_orderBtn text-right" data-order="volume' +
      (order ? "," + order : "") +
      '">成交量(手)<span class="sort_btn"><em class="caret_up  ' +
      (name == "volume" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "volume" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    hearderHtml +=
      '<th class="js_orderBtn text-right" data-order="amount' +
      (order ? "," + order : "") +
      '">成交额(' +
      (reportType == "4" ? "亿" : "万") +
      '元)<span class="sort_btn"><em class="caret_up  ' +
      (name == "amount" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "amount" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    hearderHtml +=
      '<th class="js_orderBtn text-right" data-order="prev_close' +
      (order ? "," + order : "") +
      '">前收<span class="sort_btn"><em class="caret_up  ' +
      (name == "prev_close" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "prev_close" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    hearderHtml +=
      '<th class="js_orderBtn text-right" data-order="open' +
      (order ? "," + order : "") +
      '">开盘<span class="sort_btn"><em class="caret_up  ' +
      (name == "open" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "open" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    hearderHtml +=
      '<th class="js_orderBtn text-right" data-order="high' +
      (order ? "," + order : "") +
      '">最高<span class="sort_btn"><em class="caret_up  ' +
      (name == "high" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "high" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    hearderHtml +=
      '<th class="js_orderBtn text-right" data-order="low' +
      (order ? "," + order : "") +
      '">最低<span class="sort_btn"><em class="caret_up  ' +
      (name == "low" && order == "asc" ? "fill" : "") +
      '"></em><em class="caret_down ' +
      (name == "low" && order == "desc" ? "fill" : "") +
      '"></em></span></th>';
    hearderHtml += "</tr></thead>";
    return hearderHtml;
  },
};
if ($(".js_report").length > 0) {
  report.init();
}
//行情信息-行情报表 END

/**
 * 行情走势
 */
var marketTrend = {
  marketCode: "000001",
  _lineType: 0,
  // typeArr: [
  //   { "name": "分时线", value: "0" },
  //   { "name": "日K线", value: "1" }
  // ],
  init: function () {
    this.loadEvents();
    marketLineIniMethod(this._lineType, true);
  },
  loadEvents: function () {
    var _this = this;
    $("#inputCode").val(_this.marketCode);
    triggerSearch(_this.setMarketTrend);
    //刷新按钮
    $(".btn_refresh").on("click", function () {
      marketLineIniMethod(_this._lineType, true);
    });
  },
  //设置对应行情走势图
  setMarketTrend: function () {
    var _this = marketTrend;
    // var typeVal = $(".js_marketType .selectpicker").val();
    // _this._lineType = typeVal;
    marketLineIniMethod(_this._lineType, true);
  },
};
if ($("#market_ticker").length > 0) {
  require(["highstock"], function () {
    marketTrend.init();
  });
}
//行情走势 end
