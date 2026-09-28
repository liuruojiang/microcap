/* 首页-数据-股票-成交概况-每周概况 */
var $stockweek = $(".search_stockweek");
/* 首页-数据-股票-成交概况-单日 */
var $stockday = $(".search_stockday");
var buttonStockday = $stockday.find("#btnQuery");
/* 首页-数据-股票-成交概况-月度 */
var $stockmonth = $(".search_mix");
var buttonStockmonth = $stockmonth.find("#btnQuery");
/* 首页-数据-股票-成交概况-年度 */
var $stock = $(".search_stock");
var buttonStock = $stock.find("#btnQuery");

// x为星期，值为1-7，分别代表周一到周日
function getXDayString(dateStr, x) {
    var nowDate = new Date(dateStr);
    var nowDay = nowDate.getDay() ? nowDate.getDay() : 7;
    var nowRi = nowDate.getDate();
    nowDate.setDate(nowRi + x - nowDay);
    return turnDateToString(nowDate);
}
// 处理时间格式化
function dateReplace(date) {
    return date ? date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6) : '-'
}
// 成交概况新页面赋值 - 2021.4.12
var $fundday_new = $(".search_fundday_new");
var buttonFundday_new = $fundday_new.find("#btnQuery");
var $fundmonth_new = $(".search_fundmonth_new");
var buttonFundmonth_new = $fundmonth_new.find("#btnQuery");
var $fundyear_new = $(".search_fundyear_new");
var buttonFundyear_new = $fundyear_new.find("#btnQuery");
var $fundweek_new = $(".search_fundweek_new");
String.prototype.num_toFixed = function(num) {
    var _this = ""
    if (Number(this)) {
        if (Number(this) === 0) {
            _this = '0';
        } else {
            _this = Number(this).toFixed(num);
        }
    } else {
        _this = this;
    }

    return _this
}

// Date类型的日期转换成YYYY-MM-DD
function turnDateToString(turnDate) {
    var year = turnDate.getFullYear();
    var month = (turnDate.getMonth() + 1).toString();
    var day = (turnDate.getDate()).toString();
    if (month.length == 1) {
        month = "0" + month;
    }
    if (day.length == 1) {
        day = "0" + day;
    }
    var dateTime = year + "-" + month + "-" + day;
    return dateTime;
}

function ifZeroTurn(str) {
    if (str == null || str == "" || str == undefined) {
        return "-";
    } else {
        return str;
    }
}

function ifundefindTurn(str) {
    return ifZeroTurn(str);
}
//给年下拉框赋值 - 包含新页面 2021.4.12
if ($stockmonth.length > 0 || $stock.length > 0 || $fundmonth_new.length > 0 || $fundyear_new.length > 0) {
    var myDate = new Date();
    //var year = myDate.getFullYear();
    var year = get_systemDate_global().substring(0, 4);
    var sysYearDate = '<option value="' + year + '" selected="true">' + year + '年</option>'; //start
    for (var i = year - 1; i >= 1999; --i) {
        sysYearDate += '<option value="' + i + '">' + i + '年</option>';
    }
    if ($fundyear_new.length > 0) {
        $("#single_select_2").html(sysYearDate);
    } else if ($stock.length > 0) {
        $("#single_select_2").html(sysYearDate);
    } else if ($fundmonth_new.length > 0) {
        $("#single_select_2").html(sysYearDate);
    } else if ($stockmonth.length > 0) {
        $("#single_select_2").html(sysYearDate);
    } else {
        $("#year_select").html(sysYearDate);
    }

    require(['multipleselect'], function() {
        $("#single_select_2").multipleSelect({
            width: '100%',
            selectAll: false,
            single: true,
            multipleWidth: false,
            maxHeight: 250,
            placeholder: "",
            countSelected: false,
            allSelected: false,
            onClick: function(obj) {
                if (typeof(tableFun) != 'undefined') {
                    var objFun = tableFun[obj.label];
                    if (objFun != undefined) {
                        objFun();
                    }
                }
            }
        });
    });
}

function ajaxFn(obj) {
    showloading();
    $.ajax({
        url: sseQueryURL + 'commonQuery.do',
        type: 'post',
        async: false,
        cache: false,
        dataType: "jsonp",
        jsonp: "jsonCallBack",
        jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
        data: obj.parms,
        success: function(data) {
            obj.fnCallBack(data);
        },
        complete: function() {
            hideloading();
        }
    })
}
//日股票成交概况
if ($stockday.length > 0) {
    var init = true;
    var day = '';
    // 添加跳转链接以及备注
    var htm = "<a href='/market/stockdata/overview/day/' target='_blank' style='font-size:12px;font-weight:normal;float:inherit' title='数据截止到2022年1月3日' target='_blank'>(此栏目为历史数据，更多数据点击此处)</a>"
    $(".sse_title_common").find("h2").html("每日股票情况" + htm);
    var stockDay = {
        parms: {
            searchDate: init ? '' : day,
            sqlId: 'COMMON_SSE_SJ_GPSJ_CJGK_DAYCJGK_C',
            stockType: '90'
        },
        fnCallBack: function(data) {
            var item = data.result;
            if (init && item && item[0]) {
                $("#start_date2").val(item[0].CAL_DATE.substr(0, 10));
                $(".sse_table_title2").show().find("p").html('数据日期：' + item[0].CAL_DATE.substr(0, 10));
            }
            var noData = arrayObjNodata(item, ['PRODUCT_TYPE', 'CAL_DATE']);
            var header = [
                ["", "<div class='th_div_center'>单日情况</div>"],
                ["", "<div class='th_div_center'>股票</div>"],
                ["", "<div class='th_div_center'>主板A</div>"],
                ["", "<div class='th_div_center'>主板B</div>"],
                ["", "<div class='th_div_center'>科创板</div>"],
                ["", "<div class='th_div_center'>股票回购</div>"]
            ];
            var tempArr = [];
            var headerlength = header.length;
            tempArr.push("<tr>");
            for (var j = 0; j < header.length; ++j) {
                tempArr.push("<th>" + header[j][1] + "</th>");
            }
            tempArr.push("</tr>");

            if (!item || noData) {
                tempArr.push("<tr><td colspan='50'>暂无数据</td></tr>");
            } else {
                function createArr(item) {
                    var arr = [];
                    arr[0] = item.TX_VOLUME; //成交量
                    arr[1] = item.TX_AMOUNT; //成交金额
                    arr[2] = item.TRADING_TX; //成交笔数
                    arr[3] = item.CAL_DATE; //日期
                    arr[4] = item.TX_NUM; //挂牌数
                    arr[5] = item.MKT_VALUE; //市价总值
                    arr[6] = item.NEGOTIABLE_VALUE; //流通市值
                    arr[7] = item.AVG_PROFIT_RATE; //平均市盈率
                    arr[8] = item.TOTAL_MK_CAP_RATE.num_toFixed(4); //总市值换手率
                    arr[9] = item.SUB_NEW_STOCK_RATE.num_toFixed(4); //次新换手率
                    arr[10] = item.EXCHANGE_RATE.num_toFixed(4); //流通换手率
                    return arr;
                }
                var arrA = [],
                    arrB = [],
                    arrC = [],
                    arrF = [],
                    arrG = [];
                for (var i = 0; i < item.length; i++) {
                    var result = item[i];
                    if (result.PRODUCT_TYPE == "40") {
                        arrA = createArr(result);
                    } else if (result.PRODUCT_TYPE == "1") {
                        arrB = createArr(result);
                    } else if (result.PRODUCT_TYPE == "2") {
                        arrC = createArr(result);
                    } else if (result.PRODUCT_TYPE == "43") {
                        arrF = createArr(result);
                    } else if (result.PRODUCT_TYPE == "48") {
                        arrG = createArr(result);
                    }
                }
                var list = [
                    ['挂牌数', '<div class="align_right">' + ifundefindTurn(arrA[4]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[4]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[4]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[4]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[4]) + '</div>'],
                    ['市价总值(亿元)', '<div class="align_right">' + ifundefindTurn(arrA[5]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[5]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[5]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[5]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[5]) + '</div>'],
                    ['流通市值(亿元)', '<div class="align_right">' + ifundefindTurn(arrA[6]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[6]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[6]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[6]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[6]) + '</div>'],
                    ['成交金额(亿元)', '<div class="align_right">' + ifundefindTurn(arrA[1]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[1]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[1]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[1]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[1]) + '</div>'],
                    ['成交量(亿股/亿份)', '<div class="align_right">' + ifundefindTurn(arrA[0]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[0]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[0]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[0]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[0]) + '</div>'],
                    ['成交笔数(万笔)', '<div class="align_right">' + ifundefindTurn(arrA[2]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[2]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[2]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[2]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[2]) + '</div>'],
                    ['平均市盈率(倍)', '<div class="align_right">' + ifundefindTurn(arrA[7]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[7]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[7]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[7]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[7]) + '</div>'],
                    ['换手率(%)', '<div class="align_right">' + ifundefindTurn(arrA[8]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[8]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[8]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[8]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[8]) + '</div>'],
                    ['次新股换手率(%)', '<div class="align_right">' + ifundefindTurn(arrA[9]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[9]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[9]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[9]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[9]) + '</div>'],
                    ['流通换手率(%)', '<div class="align_right">' + ifundefindTurn(arrA[10]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[10]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[10]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[10]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[10]) + '</div>']
                ];

                //创建表格内容
                var listLen = list.length;
                for (var k = 0; k < listLen; ++k) {
                    var items = list[k];
                    tempArr.push("<tr>");
                    for (var l = 0; l < items.length; ++l) {
                        var item = items[l]
                        tempArr.push("<td>" + item + "</td>");
                    }
                    tempArr.push("</tr>");
                }
            }
            $('.js_tableT01').find('.table').html(tempArr.join(""));
        }
    }
    ajaxFn(stockDay);
    buttonStockday.on("click", function() {
        init = false;
        day = $("#start_date2").val();
        stockDay.parms.searchDate = day;
        $(".sse_table_title2").show().find("p").html('数据日期：' + day);
        ajaxFn(stockDay);
    });
}

// 月股票成交概况
if ($stockmonth.length > 0) {

    //从webservice获取时间
    searchMonth1 = searchMonth.substring(searchMonth.lastIndexOf("-") + 1);
    searchMonth2 = searchMonth.substring(0, 4);
    year = searchMonth2;
    month = searchMonth1;
    var month2 = month;
    if (month != 10) {
        month2 = month.replace(0, '');
    }
    // 添加跳转链接以及备注
    var htm = "<a href='/market/stockdata/overview/monthly/' style='font-size:12px;font-weight:normal;float:inherit' title='数据截止到2021年11月' target='_blank'>(此栏目为历史数据，更多数据点击此处)</a>"
    $(".sse_title_common").find("h2").html("月度股票情况" + htm);
    //给下拉框赋值
    $("#single_select_2").find("option").attr("selected", false);
    $("#month_select").find("option[value='" + searchMonth1 + "']").attr("selected", true);
    $("#month_select").next().find("span").html(month2 + "月");
    $("#single_select_2").find("option[value='" + searchMonth2 + "']").attr("selected", true);
    $(".sse_table_title2").show().find("p").html("数据日期：" + year + "-" + month);
    deschtmlshow('mon', searchMonth, $('.sse_table_conment'));
    var stockmonth = {
        parms: {
            inYear: searchMonth,
            sqlId: 'COMMON_SSE_SJ_GPSJ_CJGK_MONTHCJGK_C',
            stockType: '90'
        },
        fnCallBack: function(data) {
            var result = data.result;
            var noData = arrayObjNodata(result, ['MTOTALTXDATE', 'PRODUCT_TYPE']);
            var header = [
                ["", "<div class='th_div_center'>月度情况</div>"],
                ["", "<div class='th_div_center'>股票</div>"],
                ["", "<div class='th_div_center'>主板A</div>"],
                ["", "<div class='th_div_center'>主板B</div>"],
                ["", "<div class='th_div_center'>科创板</div>"],
                ["", "<div class='th_div_center'>股票回购</div>"]
            ];
            var tempArr = [];
            var headerlength = header.length;

            tempArr.push("<tr>");
            for (var j = 0; j < header.length; ++j) {
                tempArr.push("<th>" + header[j][1] + "</th>");
            }
            tempArr.push("</tr>");

            if (!result || noData) {
                tempArr.push("<tr><td colspan='50'>没有数据！</td></tr>");
            } else {
                function createArr(item) {
                    var arr = [];
                    arr[0] = item.TRADING_TX; //成交笔数
                    arr[1] = item.MHGH_TRN; //最高笔数(万笔)
                    arr[2] = item.MLOW_TRN; //最低笔数(万笔)
                    arr[3] = item.TX_VOLUME; //成交量(亿股)
                    arr[4] = item.MHGH_VOL; //最高成交量(亿股)
                    arr[5] = item.MLOW_VOL; //最低成交量(亿股)
                    arr[6] = item.TX_AMOUNT; //成交金额(亿元)
                    arr[7] = item.MHGH_VAL; //最高成交金额(亿元)
                    arr[8] = item.MLOW_VAL; //最低成交金额(亿元)
                    arr[9] = item.MHGH_TRND; //最高笔数对应日期
                    arr[10] = item.MLOW_TRND; //最低笔数对应日期
                    arr[11] = item.MHGH_VOLD; //最高成交量对应日期
                    arr[12] = item.MLOW_VOLD; //最低成交量对应日期
                    arr[13] = item.MHGH_VALD; //最高成交金额对应日期
                    arr[14] = item.MLOW_VALD; //最低成交金额对应日期
                    arr[15] = item.TOT_TRD_DATE; //累计交易天数(天)
                    arr[16] = item.AVG_PROFIT_RATE; //平均市盈率(倍)
                    arr[17] = item.MKT_VALUE; //市价总值(亿元)
                    arr[18] = item.NEGOTIABLE_VALUE; //流通市值(亿元)
                    arr[20] = item.TX_NUM; //挂牌数
                    arr[19] = item.TOTAL_MK_CAP_RATE.num_toFixed(4); //总市值换手率
                    arr[21] = item.SUB_NEW_STOCK_RATE.num_toFixed(4); //次新换手率
                    arr[22] = item.EXCHANGE_RATE.num_toFixed(4); //流通换手率
                    return arr;
                }
                var arrA = [],
                    arrB = [],
                    arrC = [],
                    arrHg = [],
                    arrU = [];
                for (var i = 0; i < result.length; i++) {
                    var item = result[i];
                    var type;
                    if (item.PRODUCT_TYPE == '-' || item.PRODUCT_TYPE == undefined) {
                        type = item.PRODUCT_TYPE_B;
                    } else {
                        type = item.PRODUCT_TYPE;
                    }
                    if (type == "40") {
                        arrA = createArr(item);
                    } else if (type == "1") {
                        arrB = createArr(item);
                    } else if (type == "2") {
                        arrC = createArr(item);
                    } else if (type == "43") {
                        arrHg = createArr(item);
                    } else if (type == "48") {
                        arrU = createArr(item);
                    }
                }
                var list = [
                    ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrA[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[20]) + '</div>'],
                    ['市价总值(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[17]) + '</div>'],
                    ['流通市值(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[18]) + '</div>'],
                    ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[6]) + '</div>'],
                    ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[7]) + '</br>(' + ifZeroTurn(arrA[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[7]) + '</br>(' + ifZeroTurn(arrB[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[7]) + '</br>(' + ifZeroTurn(arrC[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[7]) + '</br>(' + ifZeroTurn(arrU[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[7]) + '</br>(' + ifZeroTurn(arrHg[13]) + ')' + '</div>'],
                    ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[8]) + '</br>(' + ifZeroTurn(arrA[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[8]) + '</br>(' + ifZeroTurn(arrB[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[8]) + '</br>(' + ifZeroTurn(arrC[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[8]) + '</br>(' + ifZeroTurn(arrU[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[8]) + '</br>(' + ifZeroTurn(arrHg[14]) + ')' + '</div>'],
                    ['成交量(亿股/亿份)', '<div class="align_right">' + ifZeroTurn(arrA[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[3]) + '</div>'],
                    ['最高成交量(亿股/亿份)', '<div class="align_right">' + ifZeroTurn(arrA[4]) + '</br>(' + ifZeroTurn(arrA[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[4]) + '</br>(' + ifZeroTurn(arrB[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[4]) + '</br>(' + ifZeroTurn(arrC[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[4]) + '</br>(' + ifZeroTurn(arrU[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[4]) + '</br>(' + ifZeroTurn(arrHg[11]) + ')' + '</div>'],
                    ['最低成交量(亿股/亿份)', '<div class="align_right">' + ifZeroTurn(arrA[5]) + '</br>(' + ifZeroTurn(arrA[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[5]) + '</br>(' + ifZeroTurn(arrB[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[5]) + '</br>(' + ifZeroTurn(arrC[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[5]) + '</br>(' + ifZeroTurn(arrU[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[5]) + '</br>(' + ifZeroTurn(arrHg[12]) + ')' + '</div>'],
                    ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[0]) + '</div>'],
                    ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[1]) + '</br>(' + ifZeroTurn(arrA[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[1]) + '</br>(' + ifZeroTurn(arrB[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[1]) + '</br>(' + ifZeroTurn(arrC[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[1]) + '</br>(' + ifZeroTurn(arrU[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[1]) + '</br>(' + ifZeroTurn(arrHg[9]) + ')' + '</div>'],
                    ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[2]) + '</br>(' + ifZeroTurn(arrA[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[2]) + '</br>(' + ifZeroTurn(arrB[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[2]) + '</br>(' + ifZeroTurn(arrC[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[2]) + '</br>(' + ifZeroTurn(arrU[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[2]) + '</br>(' + ifZeroTurn(arrHg[10]) + ')' + '</div>'],
                    ['平均市盈率(倍)', '<div class="align_right">' + ifZeroTurn(arrA[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[16]) + '</div>'],
                    ['换手率(%)', '<div class="align_right">' + ifZeroTurn(arrA[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[19]) + '</div>'],
                    ['次新换手率(%)', '<div class="align_right">' + ifZeroTurn(arrA[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[21]) + '</div>'],
                    ['流通换手率(%)', '<div class="align_right">' + ifZeroTurn(arrA[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[22]) + '</div>'],
                    ['累计交易天数(天)', '<div class="align_right">' + ifZeroTurn(arrA[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[15]) + '</div>']
                ];

                //创建表格内容
                var listLen = list.length;
                for (var k = 0; k < listLen; ++k) {
                    var items = list[k];
                    tempArr.push("<tr>");
                    for (var l = 0; l < items.length; ++l) {
                        var item = items[l]
                        tempArr.push("<td>" + item + "</td>");
                    }
                    tempArr.push("</tr>");
                }
            }

            $('.js_tableT01').find(".table").html(tempArr.join(""));

        }
    }
    ajaxFn(stockmonth);
    buttonStockmonth.on("click", function() {
        year = $("#single_select_2").find("option:selected").val();
        month = $("#month_select").find("option:selected").val();
        searchMonth = year + "-" + month;
        stockmonth.parms.inYear = searchMonth;
        $(".sse_table_title2").show().find("p").html("数据日期：" + year + "-" + month);
        deschtmlshow('mon', searchMonth, $('.sse_table_conment'));
        ajaxFn(stockmonth);
    });
}


//年股票成交概况
if ($stock.length > 0) {
    //下拉框赋值
    $("#single_select_2").find("option").attr("selected", false);
    $("#single_select_2").find("option[value='" + searchYear + "']").attr("selected", true);
    year = searchYear;
    $(".sse_table_title2").show().find("p").html("数据日期：" + year + "年");
    deschtmlshow('year', year, $('.sse_table_conment'));
    // 添加跳转链接以及备注
    var htm = "<a href='/market/stockdata/overview/yearly/' target='_blank' style='font-size:12px;font-weight:normal;float:inherit' title='数据截止到2020年' target='_blank'>(此栏目为历史数据，更多数据点击此处)</a>"
    $(".sse_title_common").find("h2").html("年度股票情况" + htm);
    var stockyear = {
        parms: {
            inYear: year,
            sqlId: 'COMMON_SSE_SJ_GPSJ_CJGK_YEARCJGK_C',
            stockType: '90'
        },
        fnCallBack: function(data) {
            var result = data.result;
            var noData = arrayObjNodata(result, ['year', 'PRODUCT_TYPE']);
            var header = [
                ["", "<div class='th_div_center'>年度情况</div>"],
                ["", "<div class='th_div_center'>股票</div>"],
                ["", "<div class='th_div_center'>主板A</div>"],
                ["", "<div class='th_div_center'>主板B</div>"],
                ["", "<div class='th_div_center'>科创板</div>"],
                ["", "<div class='th_div_center'>股票回购</div>"]
            ];
            var tempArr = [];
            var headerlength = header.length;

            tempArr.push("<tr>");
            for (var j = 0; j < header.length; ++j) {
                tempArr.push("<th>" + header[j][1] + "</th>");
            }
            tempArr.push("</tr>");

            if (!result || noData) {
                tempArr.push("<tr><td colspan='50'>没有数据！</td></tr>");
            } else {
                function createArr(item) {
                    var arr = [];
                    arr[0] = item.YTRADING_TX; //成交笔数(万笔)
                    arr[1] = item.YHGH_TRN; //最高笔数(万笔)
                    arr[2] = item.YLOW_TRN; //最低笔数(万笔)
                    arr[3] = item.YTX_VOLUME; //成交量(亿股)
                    arr[4] = item.YHGH_VOL; //最高成交量(亿股)
                    arr[5] = item.YLOW_VOL; //最低成交量(亿股)
                    arr[6] = item.YTX_AMOUNT; //成交金额(亿元)
                    arr[7] = item.YHGH_VAL; //最高成交金额(亿元)
                    arr[8] = item.YLOW_VAL; //最高成交金额(亿元)
                    arr[9] = item.YHGH_TRND; //最高笔数对应日期
                    arr[10] = item.YLOW_TRND; //最低笔数对应日期
                    arr[11] = item.YHGH_VOLD; //最高成交量对应日期
                    arr[12] = item.YLOW_VOLD; //最低成交量对应日期
                    arr[13] = item.YHGH_VALD; //最高成交金额对应日期
                    arr[14] = item.YLOW_VALD; //最低成交金额对应日期
                    arr[15] = item.YTX_DATES; //累计交易天数(天)
                    arr[16] = item.AVG_PROFIT_RATE; //平均市盈率(倍)
                    arr[17] = item.MKT_VALUE; //市价总值(亿元)
                    arr[18] = item.NEGOTIABLE_VALUE; //流通市值(亿元)
                    arr[19] = item.TOTAL_MK_CAP_RATE.num_toFixed(4); //总市值换手率
                    arr[21] = item.SUB_NEW_STOCK_RATE.num_toFixed(4); //次新换手率
                    arr[22] = item.EXCHANGE_RATE.num_toFixed(4); //流通换手率
                    arr[20] = item.TX_NUM; //挂牌数
                    return arr;
                }
                var arrA = [],
                    arrB = [],
                    arrC = [],
                    arrHg = [],
                    arrU = [];
                for (var i = 0; i < result.length; i++) {
                    var item = result[i];
                    var type;
                    if (item.PRODUCT_TYPE == '-' || item.PRODUCT_TYPE == undefined) {
                        type = item.PRODUCT_TYPE_B;
                    } else {
                        type = item.PRODUCT_TYPE;
                    }
                    if (type == "40") {
                        arrA = createArr(item);
                    } else if (type == "1") {
                        arrB = createArr(item);
                    } else if (type == "2") {
                        arrC = createArr(item);
                    } else if (type == "43") {
                        arrHg = createArr(item);
                    } else if (type == "48") {
                        arrU = createArr(item);
                    }
                }
                var list = [
                    ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrA[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[20]) + '</div>'],
                    ['市价总值(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[17]) + '</div>'],
                    ['流通市值(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[18]) + '</div>'],
                    ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[6]) + '</div>'],
                    ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[7]) + '</br>(' + ifZeroTurn(arrA[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[7]) + '</br>(' + ifZeroTurn(arrB[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[7]) + '</br>(' + ifZeroTurn(arrC[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[7]) + '</br>(' + ifZeroTurn(arrU[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[7]) + '</br>(' + ifZeroTurn(arrHg[13]) + ')' + '</div>'],
                    ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[8]) + '</br>(' + ifZeroTurn(arrA[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[8]) + '</br>(' + ifZeroTurn(arrB[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[8]) + '</br>(' + ifZeroTurn(arrC[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[8]) + '</br>(' + ifZeroTurn(arrU[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[8]) + '</br>(' + ifZeroTurn(arrHg[14]) + ')' + '</div>'],
                    ['成交量(亿股/亿份)', '<div class="align_right">' + ifZeroTurn(arrA[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[3]) + '</div>'],
                    ['最高成交量(亿股/亿份)', '<div class="align_right">' + ifZeroTurn(arrA[4]) + '</br>(' + ifZeroTurn(arrA[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[4]) + '</br>(' + ifZeroTurn(arrB[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[4]) + '</br>(' + ifZeroTurn(arrC[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[4]) + '</br>(' + ifZeroTurn(arrU[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[4]) + '</br>(' + ifZeroTurn(arrHg[11]) + ')' + '</div>'],
                    ['最低成交量(亿股/亿份)', '<div class="align_right">' + ifZeroTurn(arrA[5]) + '</br>(' + ifZeroTurn(arrA[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[5]) + '</br>(' + ifZeroTurn(arrB[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[5]) + '</br>(' + ifZeroTurn(arrC[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[5]) + '</br>(' + ifZeroTurn(arrU[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[5]) + '</br>(' + ifZeroTurn(arrHg[12]) + ')' + '</div>'],
                    ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[0]) + '</div>'],
                    ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[1]) + '</br>(' + ifZeroTurn(arrA[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[1]) + '</br>(' + ifZeroTurn(arrB[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[1]) + '</br>(' + ifZeroTurn(arrC[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[1]) + '</br>(' + ifZeroTurn(arrU[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[1]) + '</br>(' + ifZeroTurn(arrHg[9]) + ')' + '</div>'],
                    ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[2]) + '</br>(' + ifZeroTurn(arrA[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[2]) + '</br>(' + ifZeroTurn(arrB[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[2]) + '</br>(' + ifZeroTurn(arrC[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[2]) + '</br>(' + ifZeroTurn(arrU[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[2]) + '</br>(' + ifZeroTurn(arrHg[10]) + ')' + '</div>'],
                    ['平均市盈率(倍)', '<div class="align_right">' + ifZeroTurn(arrA[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[16]) + '</div>'],
                    ['换手率(%)', '<div class="align_right">' + ifZeroTurn(arrA[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[19]) + '</div>'],
                    ['次新换手率(%)', '<div class="align_right">' + ifZeroTurn(arrA[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[21]) + '</div>'],
                    ['流通换手率(%)', '<div class="align_right">' + ifZeroTurn(arrA[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[22]) + '</div>'],
                    ['累计交易天数(天)', '<div class="align_right">' + ifZeroTurn(arrA[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[15]) + '</div>']
                ];

                //创建表格内容
                var listLen = list.length;
                for (var k = 0; k < listLen; ++k) {
                    var items = list[k];
                    tempArr.push("<tr>");
                    for (var l = 0; l < items.length; ++l) {
                        var item = items[l]
                        tempArr.push("<td>" + item + "</td>");
                    }
                    tempArr.push("</tr>");
                }

            }

            $('.js_tableT01').find(".table").html(tempArr.join(""));
        }
    }
    ajaxFn(stockyear);
    buttonStock.on("click", function() {
        year = $("#single_select_2").find("option:selected").val();
        stockyear.parms.inYear = year;
        $(".sse_table_title2").show().find("p").html("数据日期：" + year + "年");
        deschtmlshow('year', year, $('.sse_table_conment'));
        ajaxFn(stockyear);
    });
}

/**
 * =========================  基金成交概况新页面  =============================================
 */

//日基金成交概况 - new
if ($fundday_new.length > 0) {
    var init = true;
    var day = '';
    var fundDay = {
        parms: {
            searchDate: init ? '' : day,
            sqlId: 'COMMON_SSE_SJ_GPSJ_CJGK_DAYCJGK_C',
            fundType: '47'
        },
        fnCallBack: function(data) {
            var result = data.result;
            if (init && result && result[0]) {
                $("#start_date2").val(result[0].CAL_DATE.substr(0, 10));
                $(".sse_table_title2").show().find("p").html('数据日期：' + result[0].CAL_DATE.substr(0, 10));
            }
            var noData = arrayObjNodata(result, ['PRODUCT_TYPE', 'CAL_DATE']);
            var header = [
                ["", "<div class='th_div_center'>单日情况</div>"],
                ["", "<div class='th_div_center'>基金</div>"],
                ["", "<div class='th_div_center'>封闭式基金</div>"],
                ["", "<div class='th_div_center'>ETF</div>"],
                ["", "<div class='th_div_center'>公募REITs</div>"],
                ["", "<div class='th_div_center'>LOF</div>"],
                ["", "<div class='th_div_center'>交易型货币基金</div>"],
                ["", "<div class='th_div_center'>基金回购</div>"]
            ];
            var tempArr = [];
            var headerlength = header.length;
            tempArr.push("<tr>");
            for (var j = 0; j < header.length; ++j) {
                tempArr.push("<th>" + header[j][1] + "</th>");
            }
            tempArr.push("</tr>");
            if (!result || noData) {
                tempArr.push("<tr><td colspan='50'>没有数据！</td></tr>");
            } else {
                function createArr(item) {
                    var arr = [];
                    arr[0] = item.TX_VOLUME_FULL; //成交量
                    arr[1] = item.TX_AMOUNT_FULL; //成交金额
                    arr[2] = item.TRADING_TX_FULL; //成交笔数
                    arr[3] = item.CAL_DATE; //日期
                    arr[4] = item.TX_NUM; //挂牌数
                    return arr;
                }
                var arrJj = [],
                    arrFb = [],
                    arrEtf = [],
                    arrReits = [],
                    arrLof = [],
                    arrJyhb = [],
                    arrJjhg = [];
                for (var i = 0; i < result.length; i++) {
                    var item = result[i];
                    if (item.PRODUCT_TYPE == "41") {
                        arrJj = createArr(item);
                    } else if (item.PRODUCT_TYPE == "3") {
                        arrFb = createArr(item);
                    } else if (item.PRODUCT_TYPE == "22") {
                        arrEtf = createArr(item);
                    } else if (item.PRODUCT_TYPE == "35") {
                        arrLof = createArr(item);
                    } else if (item.PRODUCT_TYPE == "47") {
                        arrJyhb = createArr(item);
                    } else if (item.PRODUCT_TYPE == "49") {
                        arrReits = createArr(item);
                    } else if (item.PRODUCT_TYPE == "45") {
                        arrJjhg = createArr(item);
                    }
                }

                var list = [
                    ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrJj[4]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrFb[4]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrEtf[4]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrReits[4]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrLof[4]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrJyhb[4]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrJjhg[4]) + '</div>'
                    ],

                    ['成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrJj[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrFb[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrEtf[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrReits[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrLof[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrJyhb[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrJjhg[0]) + '</div>'
                    ],

                    ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrJj[1]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrFb[1]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrEtf[1]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrReits[1]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrLof[1]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrJyhb[1]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrJjhg[1]) + '</div>'
                    ],

                    ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrJj[2]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrFb[2]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrEtf[2]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrReits[2]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrLof[2]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrJyhb[2]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrJjhg[2]) + '</div>'
                    ],
                ];
                //创建表格内容
                var listLen = list.length;
                for (var k = 0; k < listLen; ++k) {
                    var items = list[k];
                    tempArr.push("<tr>");
                    for (var l = 0; l < items.length; ++l) {
                        var item = items[l]
                        tempArr.push("<td>" + item + "</td>");
                    }
                    tempArr.push("</tr>");
                }

            }

            $('.js_tableT01').find(".table").html(tempArr.join(""));
        }
    }
    ajaxFn(fundDay);
    buttonFundday_new.on("click", function() {
        init = false;
        day = $("#start_date2").val();
        fundDay.parms.searchDate = day;
        $(
            ".sse_table_title2").show().find("p").html("数据日期：" + day);
        ajaxFn(fundDay);
    });
}

//周股票、基金成交概况 - new
if ($stockweek.length || $fundweek_new.length) {
    var $week, weekType;
    if ($stockweek.length) {
        $week = $stockweek;
        weekType = 'gp';
    } else if ($fundweek_new.length) {
        $week = $fundweek_new;
        weekType = 'jj';
    }
    var buttonWeek = $week.find("#btnQuery");

    if ($stockweek.length) {
        var day = '2021-12-24',
            monday = '2021-12-20',
            sunday = '2021-12-26';
        // 添加跳转链接以及备注
        var htm = "<a href='/market/stockdata/overview/weekly/' target='_blank' style='font-size:12px;font-weight:normal;float:inherit' title='数据截止到2022年1月3日' target='_blank'>(此栏目为历史数据，更多数据点击此处)</a>"
        $(".sse_title_common").find("h2").html("每周股票情况" + htm);
    } else {
        var day = get_systemDate_global();
        var dayNum = new Date(day).getDay();
        day = getXDayString(day, 5); //获取本周五
        if (!(dayNum == 6 || dayNum == 0)) {
            var friday = new Date(day);
            friday.setDate(friday.getDate() - 7);
            day = turnDateToString(friday); //获取上周五
        }
        var monday = getXDayString(day, 1);
        var sunday = getXDayString(day, 7);
    }

    $('.sse_table_title2').show().find('p').html('数据日期：' + monday + ' 至 ' + sunday);
    $("#start_date2").val(day);

    function weekback(data) {
        var result = data.result;
        var noData = arrayObjNodata(result, ['PRODUCT_TYPE']);
        var tempArr = [];
        switch (weekType) {
            case 'gp':
                tempArr.push('<tr><th>本周情况</th><th>股票</th><th>主板A</th><th>主板B</th><th>科创板</th><th>股票回购</th></tr>');
                break;
            case 'jj':
                tempArr.push('<tr><th>本周情况</th><th>基金</th><th>封闭式基金</th><th>ETF</th><th>公募REITs</th><th>LOF</th><th>交易型货币基金</th><th>基金回购</th></tr>');
                break;
        }
        if (!result || noData) {
            tempArr.push("<tr><td colspan='50'>暂无数据</td></tr>");
        } else {
            function createArr(item) {
                var arr = [];
                arr[0] = item.TRADING_TX; //成交笔数
                arr[1] = item.HGH_TRN; //最高成交笔数
                arr[2] = item.LOW_TRN; //最低成交笔数
                arr[3] = item.TX_VOLUME; //成交量
                arr[4] = item.HGH_VOL; //最高成交量
                arr[5] = item.LOW_VOL; //最低成交量
                arr[6] = item.TX_AMOUNT; //成交金额
                arr[7] = item.HGH_VAL; //最高成交金额
                arr[8] = item.LOW_VAL; //最低成交金额
                arr[9] = item.HGH_TRND; //最高成交笔数对应日期
                arr[10] = item.LOW_TRND; //最低成交笔数对应日期
                arr[11] = item.HGH_VOLD; //最高成交量对应日期
                arr[12] = item.LOW_VOLD; //最低成交量对应日期
                arr[13] = item.HGH_VALD; //最高成交金额对应日期
                arr[14] = item.LOW_VALD; //最低成交金额对应日期
                arr[15] = item.TX_DATES; //每周交易日天数
                arr[16] = item.AVG_PROFIT_RATE; //加权平均市盈率
                arr[17] = item.MKT_VALUE; //总市值
                arr[18] = item.NEGOTIABLE_VALUE; //流通市值
                arr[20] = item.TX_NUM; //挂牌数
                arr[19] = item.TOTAL_MK_CAP_RATE.num_toFixed(4); //总市值换手率
                arr[21] = item.SUB_NEW_STOCK_RATE.num_toFixed(4); //次新换手率
                arr[22] = item.EXCHANGE_RATE.num_toFixed(4); //流通换手率
                return arr;
            }
            var arrA = [],
                arrB = [],
                arrC = [],
                arrD = [],
                arrE = [],
                arrHg = [],
                arrF = [],
                arrS = [],
                arrT = [],
                arrU = [],
                arrV = [],
                arrW = [],
                arrR = [];

            for (var i = 0; i < result.length; i++) {
                var item = result[i];
                var type;
                if (item.PRODUCT_TYPE == '-' || item.PRODUCT_TYPE == undefined) {
                    type = item.PRODUCT_TYPE_B;
                } else {
                    type = item.PRODUCT_TYPE;
                }
                switch (type) {
                    case '12':
                        arrA = createArr(item);
                        break;
                    case '1':
                        // A股 1
                        arrB = createArr(item);
                        break;
                    case '2':
                        // B股 2
                        arrC = createArr(item);
                        break;
                    case '3':
                        //封闭式基金 3
                        arrD = createArr(item);
                        break;
                    case '22':
                        //ETF 22
                        arrE = createArr(item);
                        break;
                    case '43':
                        //43-股票回购
                        arrHg = createArr(item);
                        break;
                    case '35':
                        //LOF 35
                        arrF = createArr(item);
                        break;
                    case '45':
                        //基金回购 45
                        arrS = createArr(item);
                        break;
                    case '47':
                        //'47'-'交易型货币基金'
                        arrT = createArr(item);
                        break;
                    case '48':
                        //'48'-'科创板'
                        arrU = createArr(item);
                        break;
                    case '40':
                        // 股票
                        arrV = createArr(item);
                        break;
                    case '41':
                        // 基金
                        arrW = createArr(item);
                        break;
                    case '49':
                        //'49'-'公募REITs'
                        arrR = createArr(item);
                        break;
                }
            }
            var list = [];
            switch (weekType) {
                case 'gp':
                    list = [
                        ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrV[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[20]) + '</div>'],
                        ['市价总值(亿元)', '<div class="align_right">' + ifZeroTurn(arrV[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[17]) + '</div>'],
                        ['流通市值(亿元)', '<div class="align_right">' + ifZeroTurn(arrV[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[18]) + '</div>'],
                        ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrV[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[6]) + '</div>'],
                        ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrV[7]) + '</br>(' + ifZeroTurn(arrV[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[7]) + '</br>(' + ifZeroTurn(arrB[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[7]) + '</br>(' + ifZeroTurn(arrC[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[7]) + '</br>(' + ifZeroTurn(arrU[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[7]) + '</br>(' + ifZeroTurn(arrHg[13]) + ')' + '</div>'],
                        ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrV[8]) + '</br>(' + ifZeroTurn(arrV[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[8]) + '</br>(' + ifZeroTurn(arrB[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[8]) + '</br>(' + ifZeroTurn(arrC[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[8]) + '</br>(' + ifZeroTurn(arrU[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[8]) + '</br>(' + ifZeroTurn(arrHg[14]) + ')' + '</div>'],
                        ['成交量(亿股/亿份)', '<div class="align_right">' + ifZeroTurn(arrV[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[3]) + '</div>'],
                        ['最高成交量(亿股/亿份)', '<div class="align_right">' + ifZeroTurn(arrV[4]) + '</br>(' + ifZeroTurn(arrV[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[4]) + '</br>(' + ifZeroTurn(arrB[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[4]) + '</br>(' + ifZeroTurn(arrC[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[4]) + '</br>(' + ifZeroTurn(arrU[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[4]) + '</br>(' + ifZeroTurn(arrHg[11]) + ')' + '</div>'],
                        ['最低成交量(亿股/亿份)', '<div class="align_right">' + ifZeroTurn(arrV[5]) + '</br>(' + ifZeroTurn(arrV[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[5]) + '</br>(' + ifZeroTurn(arrB[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[5]) + '</br>(' + ifZeroTurn(arrC[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[5]) + '</br>(' + ifZeroTurn(arrU[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[5]) + '</br>(' + ifZeroTurn(arrHg[12]) + ')' + '</div>'],
                        ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrV[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[0]) + '</div>'],
                        ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrV[1]) + '</br>(' + ifZeroTurn(arrV[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[1]) + '</br>(' + ifZeroTurn(arrB[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[1]) + '</br>(' + ifZeroTurn(arrC[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[1]) + '</br>(' + ifZeroTurn(arrU[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[1]) + '</br>(' + ifZeroTurn(arrHg[9]) + ')' + '</div>'],
                        ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrV[2]) + '</br>(' + ifZeroTurn(arrV[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[2]) + '</br>(' + ifZeroTurn(arrB[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[2]) + '</br>(' + ifZeroTurn(arrC[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[2]) + '</br>(' + ifZeroTurn(arrU[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[2]) + '</br>(' + ifZeroTurn(arrHg[10]) + ')' + '</div>'],
                        ['平均市盈率(倍)', '<div class="align_right">' + ifZeroTurn(arrV[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[16]) + '</div>'],
                        ['换手率(%)', '<div class="align_right">' + ifZeroTurn(arrV[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[19]) + '</div>'],
                        ['次新股换手率(%)', '<div class="align_right">' + ifZeroTurn(arrV[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[21]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[21]) + '</div>'],
                        ['流通换手率(%)', '<div class="align_right">' + ifZeroTurn(arrV[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[22]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[22]) + '</div>'],
                        ['累计交易天数(天)', '<div class="align_right">' + ifZeroTurn(arrV[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[15]) + '</div>']
                    ];
                    break;
                case 'jj':
                    list = [
                        ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrW[20]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrD[20]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrE[20]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrR[20]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrF[20]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrT[20]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrS[20]) + '</div>'
                        ],
                        ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrW[6]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrD[6]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrE[6]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrR[6]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrF[6]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrT[6]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrS[6]) + '</div>'
                        ],
                        ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrW[7]) + '</br>(' + ifZeroTurn(arrW[13]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrD[7]) + '</br>(' + ifZeroTurn(arrD[13]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrE[7]) + '</br>(' + ifZeroTurn(arrE[13]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrR[7]) + '</br>(' + ifZeroTurn(arrR[13]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrF[7]) + '</br>(' + ifZeroTurn(arrF[13]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrT[7]) + '</br>(' + ifZeroTurn(arrT[13]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrS[7]) + '</br>(' + ifZeroTurn(arrS[13]) + ')' + '</div>'
                        ],
                        ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrW[8]) + '</br>(' + ifZeroTurn(arrW[14]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrD[8]) + '</br>(' + ifZeroTurn(arrD[14]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrE[8]) + '</br>(' + ifZeroTurn(arrE[14]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrR[8]) + '</br>(' + ifZeroTurn(arrR[14]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrF[8]) + '</br>(' + ifZeroTurn(arrF[14]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrT[8]) + '</br>(' + ifZeroTurn(arrT[14]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrS[8]) + '</br>(' + ifZeroTurn(arrS[14]) + ')' + '</div>'
                        ],
                        ['成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrW[3]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrD[3]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrE[3]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrR[3]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrF[3]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrT[3]) + '</div>' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrS[3]) + '</div>'
                        ],
                        ['最高成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrW[4]) + '</br>(' + ifZeroTurn(arrW[11]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrD[4]) + '</br>(' + ifZeroTurn(arrD[11]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrE[4]) + '</br>(' + ifZeroTurn(arrE[11]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrR[4]) + '</br>(' + ifZeroTurn(arrR[11]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrF[4]) + '</br>(' + ifZeroTurn(arrF[11]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrT[4]) + '</br>(' + ifZeroTurn(arrT[11]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrS[4]) + '</br>(' + ifZeroTurn(arrS[11]) + ')' + '</div>'
                        ],
                        ['最低成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrW[5]) + '</br>(' + ifZeroTurn(arrW[12]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrD[5]) + '</br>(' + ifZeroTurn(arrD[12]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrE[5]) + '</br>(' + ifZeroTurn(arrE[12]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrR[5]) + '</br>(' + ifZeroTurn(arrR[12]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrF[5]) + '</br>(' + ifZeroTurn(arrF[12]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrT[5]) + '</br>(' + ifZeroTurn(arrT[12]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrS[5]) + '</br>(' + ifZeroTurn(arrS[12]) + ')' + '</div>'
                        ],
                        ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrW[0]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrD[0]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrE[0]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrR[0]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrF[0]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrT[0]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrS[0]) + '</div>'
                        ],
                        ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrW[1]) + '</br>(' + ifZeroTurn(arrW[9]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrD[1]) + '</br>(' + ifZeroTurn(arrD[9]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrE[1]) + '</br>(' + ifZeroTurn(arrE[9]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrR[1]) + '</br>(' + ifZeroTurn(arrR[9]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrF[1]) + '</br>(' + ifZeroTurn(arrF[9]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrT[1]) + '</br>(' + ifZeroTurn(arrT[9]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrS[1]) + '</br>(' + ifZeroTurn(arrS[9]) + ')' + '</div>'
                        ],
                        ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrW[2]) + '</br>(' + ifZeroTurn(arrW[10]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrD[2]) + '</br>(' + ifZeroTurn(arrD[10]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrE[2]) + '</br>(' + ifZeroTurn(arrE[10]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrR[2]) + '</br>(' + ifZeroTurn(arrR[10]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrF[2]) + '</br>(' + ifZeroTurn(arrF[10]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrT[2]) + '</br>(' + ifZeroTurn(arrT[10]) + ')' + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrS[2]) + '</br>(' + ifZeroTurn(arrS[10]) + ')' + '</div>'
                        ],
                        ['累计交易天数(天)', '<div class="align_right">' + ifZeroTurn(arrW[15]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrD[15]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrE[15]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrR[15]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrF[15]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrT[15]) + '</div>',
                            '<div class="align_right">' + ifZeroTurn(arrS[15]) + '</div>'
                        ]
                    ];
                    break;
            }

            //创建表格内容
            var listLen = list.length;
            for (var k = 0; k < listLen; ++k) {
                var items = list[k];
                tempArr.push("<tr>");
                for (var l = 0; l < items.length; ++l) {
                    var item = items[l]
                    tempArr.push("<td>" + item + "</td>");
                }
                tempArr.push("</tr>");
            }
        }
        $('.js_tableT01').find(".table").html(tempArr.join(""));

    }


    var stockweek = {
        parms: {
            startDate: monday,
            endDate: sunday,
            sqlId: 'COMMON_SSE_SJ_GPSJ_CJGK_WEEKCJGK_C',
            stockType: '90'
        },
        fnCallBack: function(data) {
            weekback(data);
        }
    }
    var fundweek = {
        parms: {
            startDate: monday,
            endDate: sunday,
            sqlId: 'COMMON_SSE_SJ_GPSJ_CJGK_WEEKCJGK_C',
            fundType: '47'
        },
        fnCallBack: function(data) {
            weekback(data);
        }
    }
    if ($stockweek.length) {
        ajaxFn(stockweek);
        buttonWeek.on("click", function() {
            day = $("#start_date2").val();
            monday = getXDayString(day, 1);
            sunday = getXDayString(day, 7);
            stockweek.parms.startDate = monday;
            stockweek.parms.endDate = sunday;
            $('.sse_table_title2').show().find('p').html('数据日期：' + monday + ' 至 ' + sunday);
            ajaxFn(stockweek);
        });
    }
    if ($fundweek_new.length) {
        ajaxFn(fundweek);
        buttonWeek.on("click", function() {
            day = $("#start_date2").val();
            monday = getXDayString(day, 1);
            sunday = getXDayString(day, 7);
            fundweek.parms.startDate = monday;
            fundweek.parms.endDate = sunday;
            $('.sse_table_title2').show().find('p').html('数据日期：' + monday + ' 至 ' + sunday);
            ajaxFn(fundweek);
        });
    }
}

//月基金成交概况 - new
if ($fundmonth_new.length > 0) {
    searchMonthJ1 = searchMonthJ.substring(searchMonthJ.lastIndexOf("-") + 1);
    searchMonthJ2 = searchMonthJ.substring(0, 4);

    year = searchMonthJ2;
    month = searchMonthJ1;
    if (month == 10) {
        var month2 = month;
    } else {
        var month2 = month.replace(0, '');
    }

    $("#single_select_2").find("option").attr("selected", false);
    $("#month_select").find("option[value='" + searchMonthJ1 + "']").attr("selected", true);
    $("#month_select").next().find("span").html(month2 + "月");
    $("#single_select_2").find("option[value='" + searchMonthJ2 + "']").attr("selected", true);
    $(".sse_table_title2").show().find("p").html("数据日期：" + year + "-" + month);
    deschtmlshow('mon', searchMonthJ, $('.sse_table_conment'));
    var fundmonth = {
        parms: {
            inYear: searchMonthJ,
            sqlId: 'COMMON_SSE_SJ_GPSJ_CJGK_MONTHCJGK_C',
            fundType: '47'
        },
        fnCallBack: function(data) {
            var result = data.result;
            var noData = arrayObjNodata(result, ['MTOTALTXDATE', 'PRODUCT_TYPE']);
            var header = [
                ["", "<div class='th_div_center'>月度情况</div>"],
                ["", "<div class='th_div_center'>基金</div>"],
                ["", "<div class='th_div_center'>封闭式基金</div>"],
                ["", "<div class='th_div_center'>ETF</div>"],
                ["", "<div class='th_div_center'>公募REITs</div>"],
                ["", "<div class='th_div_center'>LOF</div>"],
                ["", "<div class='th_div_center'>交易型货币基金</div>"],
                ["", "<div class='th_div_center'>基金回购</div>"]
            ];
            var tempArr = [];
            var headerlength = header.length;
            tempArr.push("<tr>");
            for (var j = 0; j < header.length; ++j) {
                tempArr.push("<th>" + header[j][1] + "</th>");
            }
            tempArr.push("</tr>");
            if (!result || noData) {
                tempArr.push("<tr><td colspan='50'>没有数据！</td></tr>");
            } else {
                function createArr(item) {
                    var arr = [];
                    arr[0] = item.TRADING_TX; //成交笔数(万笔)
                    arr[1] = item.MHGH_TRN; //最高笔数(万笔)
                    arr[2] = item.MLOW_TRN; //最低笔数(万笔)
                    arr[3] = item.TX_VOLUME; //成交量(亿股)
                    arr[4] = item.MHGH_VOL; //最高成交量(亿股)
                    arr[5] = item.MLOW_VOL; //最低成交量(亿股)
                    arr[6] = item.TX_AMOUNT; //成交金额(亿元)
                    arr[7] = item.MHGH_VAL; //最高成交金额(亿元)
                    arr[8] = item.MLOW_VAL; //最高成交金额(亿元)
                    arr[9] = item.MHGH_TRND; //最高笔数对应日期
                    arr[10] = item.MLOW_TRND; //最低笔数对应日期
                    arr[11] = item.MHGH_VOLD; //最高成交量对应日期
                    arr[12] = item.MLOW_VOLD; //最低成交量对应日期
                    arr[13] = item.MHGH_VALD; //最高成交金额对应日期
                    arr[14] = item.MLOW_VALD; //最低成交金额对应日期
                    arr[15] = item.TOT_TRD_DATE; //累计交易天数(天)
                    arr[16] = item.TX_NUM; //挂牌数

                    return arr;
                }
                var arrD = [],
                    arrE = [],
                    arrF = [],
                    arrH = [],
                    arrS = [],
                    arrT = []
                arrR = [];
                for (var i = 0; i < result.length; i++) {
                    var item = result[i];
                    var type;
                    if (item.PRODUCT_TYPE == '-' || item.PRODUCT_TYPE == undefined) {
                        type = item.PRODUCT_TYPE_B;
                    } else {
                        type = item.PRODUCT_TYPE;
                    }
                    if (type == "3") {
                        //封闭式基金 12
                        arrD = createArr(item);
                    } else if (type == "22") {
                        //ETF 11
                        arrE = createArr(item);
                    } else if (type == "35") {
                        //LOF 35
                        arrF = createArr(item);
                    } else if (type == "41") {
                        //基金总体 1
                        arrH = createArr(item);
                    } else if (type == "45") {
                        //基金回购
                        arrS = createArr(item);
                    } else if (type == "47") {
                        //交易型货币基金
                        arrT = createArr(item);
                    } else if (type == "49") {
                        //公募REITs
                        arrR = createArr(item);
                    }
                }
                var list = [
                    ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrH[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[16]) + '</div>'
                    ],
                    ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[6]) + '</div>'
                    ],
                    ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[7]) + '</br>(' + ifZeroTurn(arrH[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[7]) + '</br>(' + ifZeroTurn(arrD[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[7]) + '</br>(' + ifZeroTurn(arrE[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[7]) + '</br>(' + ifZeroTurn(arrR[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[7]) + '</br>(' + ifZeroTurn(arrF[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[7]) + '</br>(' + ifZeroTurn(arrT[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[7]) + '</br>(' + ifZeroTurn(arrS[13]) + ')' + '</div>'
                    ],
                    ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[8]) + '</br>(' + ifZeroTurn(arrH[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[8]) + '</br>(' + ifZeroTurn(arrD[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[8]) + '</br>(' + ifZeroTurn(arrE[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[8]) + '</br>(' + ifZeroTurn(arrR[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[8]) + '</br>(' + ifZeroTurn(arrF[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[8]) + '</br>(' + ifZeroTurn(arrT[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[8]) + '</br>(' + ifZeroTurn(arrS[14]) + ')' + '</div>'
                    ],
                    ['成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[3]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[3]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[3]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[3]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[3]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[3]) + '</div>' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[3]) + '</div>'
                    ],
                    ['最高成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[4]) + '</br>(' + ifZeroTurn(arrH[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[4]) + '</br>(' + ifZeroTurn(arrD[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[4]) + '</br>(' + ifZeroTurn(arrE[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[4]) + '</br>(' + ifZeroTurn(arrR[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[4]) + '</br>(' + ifZeroTurn(arrF[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[4]) + '</br>(' + ifZeroTurn(arrT[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[4]) + '</br>(' + ifZeroTurn(arrS[11]) + ')' + '</div>'
                    ],
                    ['最低成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[5]) + '</br>(' + ifZeroTurn(arrH[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[5]) + '</br>(' + ifZeroTurn(arrD[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[5]) + '</br>(' + ifZeroTurn(arrE[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[5]) + '</br>(' + ifZeroTurn(arrR[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[5]) + '</br>(' + ifZeroTurn(arrF[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[5]) + '</br>(' + ifZeroTurn(arrT[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[5]) + '</br>(' + ifZeroTurn(arrS[12]) + ')' + '</div>'
                    ],
                    ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[0]) + '</div>'
                    ],
                    ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[1]) + '</br>(' + ifZeroTurn(arrH[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[1]) + '</br>(' + ifZeroTurn(arrD[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[1]) + '</br>(' + ifZeroTurn(arrE[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[1]) + '</br>(' + ifZeroTurn(arrR[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[1]) + '</br>(' + ifZeroTurn(arrF[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[1]) + '</br>(' + ifZeroTurn(arrT[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[1]) + '</br>(' + ifZeroTurn(arrS[9]) + ')' + '</div>'
                    ],
                    ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[2]) + '</br>(' + ifZeroTurn(arrH[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[2]) + '</br>(' + ifZeroTurn(arrD[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[2]) + '</br>(' + ifZeroTurn(arrE[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[2]) + '</br>(' + ifZeroTurn(arrR[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[2]) + '</br>(' + ifZeroTurn(arrF[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[2]) + '</br>(' + ifZeroTurn(arrT[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[2]) + '</br>(' + ifZeroTurn(arrS[10]) + ')' + '</div>'
                    ],
                    ['累计交易天数(天)', '<div class="align_right">' + ifZeroTurn(arrH[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[15]) + '</div>'
                    ]
                ];


                //创建表格内容
                var listLen = list.length;
                for (var k = 0; k < listLen; ++k) {
                    var items = list[k];
                    tempArr.push("<tr>");
                    for (var l = 0; l < items.length; ++l) {
                        var item = items[l]
                        tempArr.push("<td>" + item + "</td>");
                    }
                    tempArr.push("</tr>");
                }

            }

            $('.js_tableT01').find(".table").html(tempArr.join(""));
        }
    }
    ajaxFn(fundmonth);
    buttonFundmonth_new.on("click", function() {
        year = $("#single_select_2").find("option:selected").val();
        month = $("#month_select").find("option:selected").val();
        searchMonthJ = year + "-" + month;
        fundmonth.parms.inYear = searchMonthJ;
        $(".sse_table_title2").show().find("p").html("数据日期：" + year + "-" + month);
        deschtmlshow('mon', searchMonthJ, $('.sse_table_conment'));
        ajaxFn(fundmonth);
    });
}

//年基金成交概况 - new
if ($fundyear_new.length > 0) {
    $("#single_select_2").find("option").attr("selected", false);
    $("#single_select_2").find("option[value='" + searchYearJ + "']").attr("selected", true);
    year = searchYearJ;
    $(".sse_table_title2").show().find("p").html("数据日期：" + year + "年");

    deschtmlshow('year', year, $('.sse_table_conment'));
    var fundyear = {
        parms: {
            inYear: year,
            sqlId: 'COMMON_SSE_SJ_GPSJ_CJGK_YEARCJGK_C',
            fundType: '47'
        },
        fnCallBack: function(data) {
            var result = data.result;
            var noData = arrayObjNodata(result, ['year', 'PRODUCT_TYPE']);
            var header = [
                ["", "<div class='th_div_center'>年度情况</div>"],
                ["", "<div class='th_div_center'>基金</div>"],
                ["", "<div class='th_div_center'>封闭式基金</div>"],
                ["", "<div class='th_div_center'>ETF</div>"],
                ["", "<div class='th_div_center'>公募REITs</div>"],
                ["", "<div class='th_div_center'>LOF</div>"],
                ["", "<div class='th_div_center'>交易型货币基金</div>"],
                ["", "<div class='th_div_center'>基金回购</div>"]
            ];
            var tempArr = [];
            var headerlength = header.length;

            tempArr.push("<tr>");
            for (var j = 0; j < header.length; ++j) {
                tempArr.push("<th>" + header[j][1] + "</th>");
            }
            tempArr.push("</tr>");

            if (!result || noData) {
                tempArr.push("<tr><td colspan='50'>没有数据！</td></tr>");
            } else {
                function createArr(item) {
                    var arr = [];
                    arr[0] = item.YTRADING_TX; //成交笔数(万笔)
                    arr[1] = item.YHGH_TRN; //最高笔数(万笔)
                    arr[2] = item.YLOW_TRN; //最低笔数(万笔)
                    arr[3] = item.YTX_VOLUME; //成交量(亿股)
                    arr[4] = item.YHGH_VOL; //最高成交量(亿股)
                    arr[5] = item.YLOW_VOL; //最低成交量(亿股)
                    arr[6] = item.YTX_AMOUNT; //成交金额(亿元)
                    arr[7] = item.YHGH_VAL; //最高成交金额(亿元)
                    arr[8] = item.YLOW_VAL; //最高成交金额(亿元)
                    arr[9] = item.YHGH_TRND; //最高笔数对应日期
                    arr[10] = item.YLOW_TRND; //最低笔数对应日期
                    arr[11] = item.YHGH_VOLD; //最高成交量对应日期
                    arr[12] = item.YLOW_VOLD; //最低成交量对应日期
                    arr[13] = item.YHGH_VALD; //最高成交金额对应日期
                    arr[14] = item.YLOW_VALD; //最低成交金额对应日期
                    arr[15] = item.YTX_DATES; //累计交易天数(天)
                    arr[16] = item.TX_NUM; //挂牌数
                    return arr;
                }
                var arrD = [],
                    arrE = [],
                    arrF = [],
                    arrH = [],
                    arrS = [],
                    arrT = [],
                    arrR = [];
                for (var i = 0; i < result.length; i++) {
                    var item = result[i];
                    var type;
                    if (item.PRODUCT_TYPE == '-' || item.PRODUCT_TYPE == undefined) {
                        type = item.PRODUCT_TYPE_B;
                    } else {
                        type = item.PRODUCT_TYPE;
                    }
                    if (type == "3") {
                        //封闭式基金 12
                        arrD = createArr(item);
                    } else if (type == "22") {
                        //ETF 11
                        arrE = createArr(item);
                    } else if (type == "35") {
                        //LOF 35
                        arrF = createArr(item);
                    } else if (type == "41") {
                        //基金总体 1
                        arrH = createArr(item);
                    } else if (type == "45") {
                        //基金回购
                        arrS = createArr(item);
                    } else if (type == "47") {
                        //交易型货币基金
                        arrT = createArr(item);
                    } else if (type == "49") {
                        //公募REITs
                        arrR = createArr(item);
                    }
                }

                var list = [
                    ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrH[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[16]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[16]) + '</div>'
                    ],
                    ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[6]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[6]) + '</div>'
                    ],
                    ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[7]) + '</br>(' + ifZeroTurn(arrH[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[7]) + '</br>(' + ifZeroTurn(arrD[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[7]) + '</br>(' + ifZeroTurn(arrE[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[7]) + '</br>(' + ifZeroTurn(arrR[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[7]) + '</br>(' + ifZeroTurn(arrF[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[7]) + '</br>(' + ifZeroTurn(arrT[13]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[7]) + '</br>(' + ifZeroTurn(arrS[13]) + ')' + '</div>'
                    ],
                    ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[8]) + '</br>(' + ifZeroTurn(arrH[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[8]) + '</br>(' + ifZeroTurn(arrD[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[8]) + '</br>(' + ifZeroTurn(arrE[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[8]) + '</br>(' + ifZeroTurn(arrR[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[8]) + '</br>(' + ifZeroTurn(arrF[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[8]) + '</br>(' + ifZeroTurn(arrT[14]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[8]) + '</br>(' + ifZeroTurn(arrS[14]) + ')' + '</div>'
                    ],
                    ['成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[3]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[3]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[3]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[3]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[3]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[3]) + '</div>' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[3]) + '</div>'
                    ],
                    ['最高成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[4]) + '</br>(' + ifZeroTurn(arrH[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[4]) + '</br>(' + ifZeroTurn(arrD[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[4]) + '</br>(' + ifZeroTurn(arrE[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[4]) + '</br>(' + ifZeroTurn(arrR[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[4]) + '</br>(' + ifZeroTurn(arrF[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[4]) + '</br>(' + ifZeroTurn(arrT[11]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[4]) + '</br>(' + ifZeroTurn(arrS[11]) + ')' + '</div>'
                    ],
                    ['最低成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[5]) + '</br>(' + ifZeroTurn(arrH[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[5]) + '</br>(' + ifZeroTurn(arrD[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[5]) + '</br>(' + ifZeroTurn(arrE[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[5]) + '</br>(' + ifZeroTurn(arrR[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[5]) + '</br>(' + ifZeroTurn(arrF[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[5]) + '</br>(' + ifZeroTurn(arrT[12]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[5]) + '</br>(' + ifZeroTurn(arrS[12]) + ')' + '</div>'
                    ],
                    ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[0]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[0]) + '</div>'
                    ],
                    ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[1]) + '</br>(' + ifZeroTurn(arrH[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[1]) + '</br>(' + ifZeroTurn(arrD[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[1]) + '</br>(' + ifZeroTurn(arrE[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[1]) + '</br>(' + ifZeroTurn(arrR[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[1]) + '</br>(' + ifZeroTurn(arrF[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[1]) + '</br>(' + ifZeroTurn(arrT[9]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[1]) + '</br>(' + ifZeroTurn(arrS[9]) + ')' + '</div>'
                    ],
                    ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[2]) + '</br>(' + ifZeroTurn(arrH[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[2]) + '</br>(' + ifZeroTurn(arrD[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[2]) + '</br>(' + ifZeroTurn(arrE[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[2]) + '</br>(' + ifZeroTurn(arrR[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[2]) + '</br>(' + ifZeroTurn(arrF[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[2]) + '</br>(' + ifZeroTurn(arrT[10]) + ')' + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[2]) + '</br>(' + ifZeroTurn(arrS[10]) + ')' + '</div>'
                    ],
                    ['累计交易天数(天)', '<div class="align_right">' + ifZeroTurn(arrH[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrD[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrE[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrR[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrF[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrT[15]) + '</div>',
                        '<div class="align_right">' + ifZeroTurn(arrS[15]) + '</div>'
                    ]
                ];
                //创建表格内容
                var listLen = list.length;
                for (var k = 0; k < listLen; ++k) {
                    var items = list[k];
                    tempArr.push("<tr>");
                    for (var l = 0; l < items.length; ++l) {
                        var item = items[l]
                        tempArr.push("<td>" + item + "</td>");
                    }
                    tempArr.push("</tr>");
                }
            }
            $('.js_tableT01').find(".table").html(tempArr.join(""));
        }
    }
    ajaxFn(fundyear);
    buttonFundyear_new.on("click", function() {
        year = $("#single_select_2").find("option:selected").val();
        fundyear.parms.inYear = year;
        $(".sse_table_title2").show().find("p").html("数据日期：" + year + "年");
        deschtmlshow('year', year, $('.sse_table_conment'));
        ajaxFn(fundyear);
    });
}