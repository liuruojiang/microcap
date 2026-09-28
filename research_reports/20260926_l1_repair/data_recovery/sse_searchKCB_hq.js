var queryParameObj = parseJsContent(col_id);
var nowpage = 1;
var currentYear = Number(new Date().format('yyyy'));

function stockIsKcb(code, cb) {
    if (code) {
        $.ajax({
            url: sseQueryURL + "commonQuery.do",
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            async: false,
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: {
                'isPagination': false,
                'sqlId': 'COMMON_SSE_ZQLX_C',
                'SEC_CODE': code
            },
            success: function(dataJson) {
                if (dataJson.result && dataJson.result[0].SUB_TYPE == "KSH") {
                    cb(true);
                } else {
                    cb(false);
                }
            }
        })
    } else {
        cb(false);
    }

}

function Fractional234(value123, n) {
    var ps = String(value123).split(".");
    if (ps.length == 1) {
        return value123;
    }
    var num = Number(value123);
    if (num == 0) {
        return num;
    }
    return num.toFixedx(n);
}

function renum(num, s) {
    s = s ? s : 0;
    var result;
    if (num == 0) {
        result = 0;
        return result;
    }
    if (num == '' || num == null || num == undefined || num == '-') {
        result = '-';
        return result;
    }
    if (Math.abs(Number(num)) < 0.005) {
        result = Number(num).toFixed(4);
        return result;
    }
    result = Number(num).toFixed(s);
    return result;
}


function shijian(nowtime, day, month, year) {
    var yeartemp = parseInt(nowtime.substring(0, 4));
    var monthtemp = parseInt(nowtime.substring(5, 7));
    var daytemp = parseInt(nowtime.substring(8, 10));
    if (parseInt(nowtime.substring(8, 10)) == 0) {
        var daytemp = parseInt(nowtime.substring(9, 10));
    }
    if (parseInt(nowtime.substring(5, 7)) == 0) {
        var monthtemp = parseInt(nowtime.substring(6, 7));
    }
    if (year > 0) {
        yeartemp = yeartemp - year;
    }
    if (month > 0) {
        for (var im = 0; im < month; im++) {
            monthtemp--;
            if (monthtemp == 0) {
                monthtemp = 12;
                yeartemp--;
            }
        }
    }
    if (day > 0) {
        for (var id123 = 0; id123 < day; id123++) {
            daytemp--;
            if (daytemp == 0) {
                monthtemp--;
                if (monthtemp == 0) {
                    monthtemp = 12;
                    yeartemp--;
                }
                switch (monthtemp) {
                    case 1:
                        daytemp = 31;
                        break;
                    case 2:
                        daytemp = 28;
                        break;
                    case 3:
                        daytemp = 31;
                        break;
                    case 4:
                        daytemp = 30;
                        break;
                    case 5:
                        daytemp = 31;
                        break;
                    case 6:
                        daytemp = 30;
                        break;
                    case 7:
                        daytemp = 31;
                        break;
                    case 8:
                        daytemp = 31;
                        break;
                    case 9:
                        daytemp = 30;
                        break;
                    case 10:
                        daytemp = 31;
                        break;
                    case 11:
                        daytemp = 30;
                        break;
                    case 12:
                        daytemp = 31;
                        break;
                    default:
                        break;
                }
            }
        }
    }
    if ((monthtemp == 2) && (daytemp > 29) && ((yeartemp % 4) == 0)) {
        daytemp = 29;
    }
    if ((monthtemp == 2) && (daytemp > 28) && ((yeartemp % 4) != 0)) {
        daytemp = 28;
    }
    if (((monthtemp == 4) || (monthtemp == 6) || (monthtemp == 9) || (monthtemp == 11)) && (daytemp > 30)) {
        daytemp = 30;
    }
    if (((monthtemp == 1) || (monthtemp == 3) || (monthtemp == 5) || (monthtemp == 7) || (monthtemp == 8) || (monthtemp == 10) || (monthtemp == 12)) && (daytemp > 30)) {
        daytemp = 31;
    }
    if (monthtemp < 10) {
        var mont = '0' + monthtemp;
    } else {
        var mont = monthtemp;
    }
    if (daytemp < 10) {
        var dddt = '0' + daytemp;
    } else {
        var dddt = daytemp;
    }
    var changetime = yeartemp + '-' + mont + '-' + dddt;
    return changetime;
}

function tofixed2(num) {
    var dateeee = num;
    if ((dateeee != '-') && (dateeee != undefined)) {
        dateeee = Fractional234(dateeee, 2);
    } else {
        dateeee = '-';
    }
    return dateeee;
}
var loadPagemjzj = function(tData, obj, ajaxHtml) {
    //var url = "data.json?pagesize=10&page=1";
    var url = tData.url;
    //初始化数据
    showloading();
    jQuery.ajax({
        url: url,
        type: "POST",
        dataType: "jsonp",
        jsonp: "jsonCallBack",
        jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
        data: tData.params,
        cache: false,
        success: function(dataJson) {
            var results = {
                "pageIndex": dataJson.pageHelp.pageNo,
                "pageCount": dataJson.pageHelp.pageCount,
                dataJson: dataJson.result
            }; //APP接口返回的数据
            //填充html页面
            if (tData.isPageing) {
                obj.pageSelect.find('.page-con-table').show();
                obj.pageSelect.find('.mobile-page').show();
            }
            if (results.pageCount == null || results.pageCount == 1 || results.pageCount == 0) {
                obj.pageSelect.find(".page-con-table").hide();

            } else {
                obj.pageSelect.find(".page-con-table").show();
            }
            ajaxHtml(tData, obj, results, tData.pageCache, dataJson);

            tdclickable();

            hideloading();
            getPage({
                pageId: obj.pageSelect, //分页输出ID选择器
                headerKeep: 1, //头部预留页码数量 headerKeep + footerKeep 必须为偶数
                footerKeep: 1, //尾部预留页码数量 headerKeep + footerKeep 必须为偶数
                pageLength: 5, //页码显示数量,必须为奇数
                tagStr: 'a', //使用标签
                tagStr2: 'button', //使用标签
                classStr: 'classStr', //标签class
                idStr: 'idStr', //标签id
                nameStr: 'nameStr', //标签name
                disable: 'disable', //不能点击class
                active: 'active', //标签选中class
                prevName: '<span aria-hidden="true"  class="glyphicon glyphicon-menu-left"></span>',
                prevName2: '上一页', //手机版的button内容
                nextName: '<span aria-hidden="true" class="glyphicon glyphicon-menu-right"></span>',
                nextName2: '下一页',
                classPage: 'classPage', //上下页class
                pageType: 'APP', //分页类型
                ajaxData: function($this) {
                    /**
                    pageIndex: 当前页码
                    pageCount：共多少页码
                    */
                    if ($this != undefined) {
                        var tempData = tableData[obj.pageSelect.attr('id')];
                        if (tempData != undefined) {
                            tData.params = tempData.params;
                        }
                        nowpage = $this.pageIndex;
                        tData.params["pageHelp.pageNo"] = $this.pageIndex;
                        tData.params["pageHelp.beginPage"] = $this.pageIndex;
                        tData.params["pageHelp.endPage"] = $this.pageIndex + 1;
                        var search_zhgg = $('.search_zhgg');
                        if (search_zhgg.length > 0) {

                            tData.params.effective_date = tempData.params.effective_date;

                        }

                        //查询数据
                        showloading();
                        jQuery.ajax({
                            url: url,
                            dataType: "jsonp",
                            jsonp: "jsonCallBack",
                            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                            data: tData.params,
                            async: false,
                            cache: false,
                            success: function(dataJson) {
                                var results = {
                                    "pageIndex": $this.pageIndex,
                                    "pageCount": dataJson.pageHelp.pageCount,
                                    dataJson: dataJson.result
                                }; //APP接口返回的数据
                                ajaxHtml(tData, obj, results, true, dataJson);

                                tdclickable();

                            },
                            complete: function() {
                                hideloading();
                            },
                            error: function(e) {}
                        });
                    } else {
                        return results;
                    }
                }
            });

        },
        error: function(e) {}
    });
};

function ifZeroTurn(str) {
    if (str == "0" || str == 0 || str == null || str == "" || str == undefined) {
        return "-";
    } else {
        return str;
    }
}

function ifundefindTurn(str) {
    return ifZeroTurn(str);
}

// function ifNullTurn(str) {
//   if (str == null || str == "" || str == undefined) {
//     return "-";
//   } else {
//     return str;
//   }
// }
function ifundefindTurn1(str) {
    if (str == null || str === "" || str == undefined) {
        return "-";
    } else {
        return str;
    }
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
// x为星期，值为1-7，分别代表周一到周日
function getXDayString(dateStr, x) {
    var nowDate = new Date(dateStr);
    var nowDay = nowDate.getDay() ? nowDate.getDay() : 7;
    var nowRi = nowDate.getDate();
    nowDate.setDate(nowRi + x - nowDay);
    return turnDateToString(nowDate);
}
var todaydata = get_systemDate_global();

function year(srartyear, $htmladd, yesss, morenzhi) {
    var year = parseInt(todaydata.substring(0, 4));
    var sysYearDate = ''; //start
    if (morenzhi == 1) {
        sysYearDate = '<option value="" selected="true">请选择</option>'
    }
    for (var i = year; i >= srartyear; --i) {
        if (yesss == i) {
            sysYearDate += '<option value="' + yesss + '" selected="true">' + yesss + '年</option>';
        } else {
            sysYearDate += '<option value="' + i + '">' + i + '年</option>';
        }
    }
    $htmladd.html(sysYearDate);
    require(['multipleselect'], function() {
        $htmladd.multipleSelect({
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



$(function() {
    /* 首页-数据-股票-成交概况-每周概况 */
    var $stockweek = $(".search_stockweek");
    /* 首页-数据-基金-成交概况-每周概况 */
    var $fundweek = $(".search_fundweek");
    /* 首页-数据-债券-成交概况-每周概况 */
    var $bondweek = $(".search_bondweek");

    /* 首页-数据-股票-成交概况-单日 */
    var $stockday = $(".search_stockday");
    var buttonStockday = $stockday.find("#btnQuery");

    /* 首页-数据-基金总体-成交概况-单日 */
    var $fundday = $(".search_fundday");
    var buttonFundday = $fundday.find("#btnQuery");

    /* 首页-数据-股票-成交概况-月度 */
    var $mix = $(".search_mix");
    var buttonMix = $mix.find("#btnQuery");

    /* 首页-数据-股票-成交概况-年度 */
    var $stock = $(".search_stock");
    var buttonStock = $stock.find("#btnQuery");

    /* 首页-数据-基金-成交概况-月度 */
    var $fundmonth = $(".search_fundmonth");
    var buttonFundmonth = $fundmonth.find("#btnQuery");


    /* 首页-数据-基金-成交概况-年度 */
    var $fundyear = $(".search_fundyear");
    var buttonFundyear = $fundyear.find("#btnQuery");

    /* 首页-数据-其他数据-港股通成交概况-单日 */
    var $ggtday = $(".search_ggtday");
    var buttonGgtday = $ggtday.find("#btnQuery");

    /* Q03Q05跳转 */
    var $rule = $(".search_ruleOld");



    //给年下拉框赋值
    if ($mix.length > 0 || $stock.length > 0 || $fundmonth.length > 0 || $fundyear.length > 0) {
        var myDate = new Date();
        //var year = myDate.getFullYear();
        var year = get_systemDate_global().substring(0, 4);
        var sysYearDate = '<option value="' + year + '" selected="true">' + year + '年</option>'; //start
        for (var i = year - 1; i >= 1999; --i) {
            sysYearDate += '<option value="' + i + '">' + i + '年</option>';
        }
        if ($fundyear.length > 0) {
            $("#single_select_2").html(sysYearDate);
        } else if ($stock.length > 0) {
            $("#single_select_2").html(sysYearDate);
        } else if ($fundmonth.length > 0) {
            $("#single_select_2").html(sysYearDate);
        } else if ($mix.length > 0) {
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
    if ($bondweek.length) {
        var day = get_systemDate_global();
        var dayNum = new Date(day).getDay();
        day = getXDayString(day, 5); //获取本周五
        if (!(dayNum == 6 || dayNum == 0)) {
            var friday = new Date(day);
            friday.setDate(friday.getDate() - 7);
            day = turnDateToString(friday); //获取上周五
        }
        $("#start_date2").val(day);
        var monday = getXDayString(day, 1);
        var sunday = getXDayString(day, 7);
        var ajaxSearch01 = function(tableData, obj, results, pageCache) {
            var dataJson = results.dataJson;
            var htmlArr = [];
            $('.sse_table_title2').show().find('p').html('数据日期：' + monday + ' 至 ' + sunday);
            htmlArr.push('<tr class="greybg"><th>类型</th><th>成交笔数</th><th>成交金额(万元)</th><th>加权平均价格</th></tr>');
            if (isBlankOrNull(dataJson)) {
                htmlArr.push("<tr><td colspan='50'>没有数据！</td></tr>");
            } else {
                for (var i = 0; i < dataJson.length; ++i) {
                    htmlArr.push('<tr><td>' + dataJson[i].TYPE + '</div></td><td><div class="align_right">' + dataJson[i].VOLUME + '</div></td><td><div class="align_right">' + dataJson[i].AMOUNT + '</div></td><td><div class="align_right">' + dataJson[i].AVG_PRICE + '</div></td></tr>');
                }
            }
            $('.table').html(htmlArr.join(""));

        }

        function searchbondweekly(obj) {

            var sqlid = "COMMON_SSE_SJ_ZQSJ_CJGK_WEEKLY";
            var action = "commonQuery";
            var tempData = {
                isPageing: true,
                url: sseQueryURL + action + ".do",
                params: {
                    "isPagination": true,
                    "sqlId": obj.sqlId,
                    "pageHelp.pageSize": 20,
                    "pageHelp.cacheSize": 1,
                    "pageHelp.pageNo": 1,
                    "pageHelp.beginPage": 1,
                    "pagecache": false,
                    "start_date": obj.start_date,
                    "end_date": obj.end_date
                }
            }
            tdclickable();
            if (tempData.isPageing) {
                loadPage(tempData, {
                    pageSelect: $('.con_block')
                }, ajaxSearch01);
            }

        }
        $("#btnQuery").on("click", function() {
            var day = $("#start_date2").val();
            monday = getXDayString(day, 1);
            sunday = getXDayString(day, 7);
            searchbondweekly({
                "sqlId": "COMMON_SSE_SJ_ZQSJ_CJGK_WEEKLY",
                "start_date": monday.replace(/-/g, ""),
                "end_date": sunday.replace(/-/g, "")
            })
        })
        searchbondweekly({
            "sqlId": "COMMON_SSE_SJ_ZQSJ_CJGK_WEEKLY",
            "start_date": monday.replace(/-/g, ""),
            "end_date": sunday.replace(/-/g, "")
        })
    }



    //股票 基金 债券  周情况
    if ($stockweek.length > 0 || $fundweek.length > 0) {
        var $week, weekType;
        if ($stockweek.length > 0) {
            $week = $stockweek;
            weekType = 'gp';
        } else if ($fundweek.length > 0) {
            $week = $fundweek;
            weekType = 'jj';
        } else if ($bondweek.length > 0) {
            $week = $bondweek;
            weekType = 'zq';
        }
        var buttonWeek = $week.find("#btnQuery");
        var day = get_systemDate_global();
        var dayNum = new Date(day).getDay();
        day = getXDayString(day, 5); //获取本周五
        if (!(dayNum == 6 || dayNum == 0)) {
            var friday = new Date(day);
            friday.setDate(friday.getDate() - 7);
            day = turnDateToString(friday); //获取上周五
        }

        $("#start_date2").val(day);
        showajaxWeek();

        buttonWeek.on("click", function() {
            day = $("#start_date2").val();
            showajaxWeek();
        });

        function showajaxWeek() {
            var monday = getXDayString(day, 1);
            var sunday = getXDayString(day, 7);
            $('.sse_table_title2').show().find('p').html('数据日期：' + monday + ' 至 ' + sunday);
            showloading();
            var action = "queryWeekTradeNew";
            $.ajax({
                url: sseQueryURL + "marketdata/tradedata/" + action + ".do",
                type: 'post',
                async: false,
                cache: false,
                dataType: "jsonp",
                jsonp: "jsonCallBack",
                jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                data: {
                    prodType: weekType,
                    startDate: monday,
                    endDate: sunday,
                },
                success: function(data) {
                    var result = data.result;
                    var noData = arrayObjNodata(result, ['productType']);
                    var tempArr = [];
                    switch (weekType) {
                        case 'gp':
                            tempArr.push('<tr><th>本周情况</th><th>股票</th><th>主板A</th><th>主板B</th><th>科创板</th><th>股票回购</th></tr>');
                            break;
                        case 'jj':
                            tempArr.push('<tr><th>本周情况</th><th>基金</th><th>封闭式基金</th><th>ETF</th><th>LOF</th><th>交易型货币基金</th><th>基金回购</th></tr>');
                            break;
                        case 'zq':
                            tempArr.push('<tr><th>本周情况</th><th>国债现货</th><th>地方政府债</th><th>公司债</th><th>企业债现货</th><th>可转债</th><th>可分离债</th><th>企业债回购</th><th>国债买断式回购</th><th>新质押式债券回购</th></tr>');
                            break;
                    }
                    if (!result || noData) {
                        tempArr.push("<tr><td colspan='50'>没有数据！</td></tr>");
                    } else {
                        function createArr(item) {
                            var arr = [];
                            arr[0] = item.tradingTx; //成交笔数610
                            arr[1] = item.hghTrn; //最高成交笔数
                            arr[2] = item.lowTrn; //最低成交笔数
                            arr[3] = item.txVolume; //成交量
                            arr[4] = item.hghVol; //最高成交量
                            arr[5] = item.lowVol; //最低成交量
                            arr[6] = item.txAmount; //成交金额
                            arr[7] = item.hghVal; //最高成交金额
                            arr[8] = item.lowVal; //最低成交金额
                            arr[9] = item.hghTrnd; //最高成交笔数对应日期
                            arr[10] = item.lowTrnd; //最低成交笔数对应日期
                            arr[11] = item.hghVold; //最高成交量对应日期
                            arr[12] = item.lowVold; //最低成交量对应日期
                            arr[13] = item.hghVald; //最高成交金额对应日期
                            arr[14] = item.lowVald; //最低成交金额对应日期
                            arr[15] = item.txDates; //每周交易日天数
                            arr[16] = item.avgProfitRate; //加权平均市盈率
                            arr[17] = item.mktValue; //总市值
                            arr[18] = item.negotiableValue; //流通市值
                            arr[19] = item.exchangeRate; //换手率（流通）
                            arr[20] = item.txNum; //挂牌数
                            return arr;
                        }
                        for (var i = 0; i < result.length; i++) {
                            var item = result[i];
                            switch (item.productType) {
                                case '12':
                                    // 股票
                                    var arrA = createArr(item);
                                    break;
                                case '1':
                                    // A股 1
                                    var arrB = createArr(item);
                                    break;
                                case '2':
                                    // B股 2
                                    var arrC = createArr(item);
                                    break;
                                case '3':
                                    //封闭式基金 3
                                    var arrD = createArr(item);
                                    break;
                                case '22':
                                    //ETF 22
                                    var arrE = createArr(item);
                                    break;
                                case '37':
                                    //37-科创板A
                                    var arrKcbA = createArr(item);
                                    break;
                                case '43':
                                    //43-股票回购
                                    var arrHg = createArr(item);
                                    break;
                                case '46':
                                    //46-科创板CDR
                                    var arrKcbC = createArr(item);
                                    break;
                                case '35':
                                    //LOF 35
                                    var arrF = createArr(item);
                                    break;
                                case '36':
                                    //分级LOF 36
                                    var arrG = createArr(item);
                                    break;
                                case '18':
                                    //基金总体 18
                                    var arrH = createArr(item);
                                    break;
                                case '6':
                                    //国债现货 6
                                    var arrI = createArr(item);
                                    break;
                                case '27':
                                    //地方政府债 27
                                    var arrJ = createArr(item);
                                    break;
                                case '25':
                                    //公司债 25
                                    var arrK = createArr(item);
                                    break;
                                case '8':
                                    //企业债现货 8
                                    var arrL = createArr(item);
                                    break;
                                case '9':
                                    //可转债 9
                                    var arrM = createArr(item);
                                    break;
                                case '28':
                                    //可分离债 28
                                    var arrN = createArr(item);
                                    break;

                                case '99':

                                    //企业债回购 99
                                    var arrO = createArr(item);
                                    break;
                                case '21':
                                    //国债买断式回购 21
                                    var arrP = createArr(item);
                                    break;
                                case '24':
                                    //新质押式债券回购 24
                                    var arrQ = createArr(item);
                                    break;
                                case '45':
                                    //基金回购 45
                                    var arrS = createArr(item);
                                    break;
                                case '47':
                                    //'47'-'交易型货币基金'
                                    var arrT = createArr(item);
                                    break;
                                case '48':
                                    //'48'-'科创板'
                                    var arrU = createArr(item);
                                    break;
                                case '40':
                                    // 股票
                                    var arrV = createArr(item);
                                    break;
                                case '41':
                                    // 股票
                                    var arrW = createArr(item);
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
                                    ['成交量(亿股)', '<div class="align_right">' + ifZeroTurn(arrV[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[3]) + '</div>'],
                                    ['最高成交量(亿股)', '<div class="align_right">' + ifZeroTurn(arrV[4]) + '</br>(' + ifZeroTurn(arrV[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[4]) + '</br>(' + ifZeroTurn(arrB[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[4]) + '</br>(' + ifZeroTurn(arrC[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[4]) + '</br>(' + ifZeroTurn(arrU[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[4]) + '</br>(' + ifZeroTurn(arrHg[11]) + ')' + '</div>'],
                                    ['最低成交量(亿股)', '<div class="align_right">' + ifZeroTurn(arrV[5]) + '</br>(' + ifZeroTurn(arrV[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[5]) + '</br>(' + ifZeroTurn(arrB[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[5]) + '</br>(' + ifZeroTurn(arrC[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[5]) + '</br>(' + ifZeroTurn(arrU[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[5]) + '</br>(' + ifZeroTurn(arrHg[12]) + ')' + '</div>'],
                                    ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrV[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[0]) + '</div>'],
                                    ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrV[1]) + '</br>(' + ifZeroTurn(arrV[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[1]) + '</br>(' + ifZeroTurn(arrB[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[1]) + '</br>(' + ifZeroTurn(arrC[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[1]) + '</br>(' + ifZeroTurn(arrU[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[1]) + '</br>(' + ifZeroTurn(arrHg[9]) + ')' + '</div>'],
                                    ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrV[2]) + '</br>(' + ifZeroTurn(arrV[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[2]) + '</br>(' + ifZeroTurn(arrB[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[2]) + '</br>(' + ifZeroTurn(arrC[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[2]) + '</br>(' + ifZeroTurn(arrU[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[2]) + '</br>(' + ifZeroTurn(arrHg[10]) + ')' + '</div>'],
                                    ['平均市盈率(倍)', '<div class="align_right">' + ifZeroTurn(arrV[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[16]) + '</div>'],
                                    ['换手率(%)', '<div class="align_right">' + ifZeroTurn(arrV[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[19]) + '</div>'],
                                    ['累计交易天数(天)', '<div class="align_right">' + ifZeroTurn(arrV[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[15]) + '</div>']
                                ];
                                break;
                            case 'jj':
                                list = [
                                    ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrW[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[20]) + '</div>'],
                                    ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrW[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[6]) + '</div>'],
                                    ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrW[7]) + '</br>(' + ifZeroTurn(arrW[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[7]) + '</br>(' + ifZeroTurn(arrD[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[7]) + '</br>(' + ifZeroTurn(arrE[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[7]) + '</br>(' + ifZeroTurn(arrF[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[7]) + '</br>(' + ifZeroTurn(arrT[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[7]) + '</br>(' + ifZeroTurn(arrS[13]) + ')' + '</div>'],
                                    ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrW[8]) + '</br>(' + ifZeroTurn(arrW[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[8]) + '</br>(' + ifZeroTurn(arrD[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[8]) + '</br>(' + ifZeroTurn(arrE[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[8]) + '</br>(' + ifZeroTurn(arrF[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[8]) + '</br>(' + ifZeroTurn(arrT[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[8]) + '</br>(' + ifZeroTurn(arrS[14]) + ')' + '</div>'],
                                    ['成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrW[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[3]) + '</div>' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[3]) + '</div>'],
                                    ['最高成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrW[4]) + '</br>(' + ifZeroTurn(arrW[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[4]) + '</br>(' + ifZeroTurn(arrD[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[4]) + '</br>(' + ifZeroTurn(arrE[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[4]) + '</br>(' + ifZeroTurn(arrF[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[4]) + '</br>(' + ifZeroTurn(arrT[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[4]) + '</br>(' + ifZeroTurn(arrS[11]) + ')' + '</div>'],
                                    ['最低成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrW[5]) + '</br>(' + ifZeroTurn(arrW[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[5]) + '</br>(' + ifZeroTurn(arrD[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[5]) + '</br>(' + ifZeroTurn(arrE[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[5]) + '</br>(' + ifZeroTurn(arrF[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[5]) + '</br>(' + ifZeroTurn(arrT[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[5]) + '</br>(' + ifZeroTurn(arrS[12]) + ')' + '</div>'],
                                    ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrW[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[0]) + '</div>'],
                                    ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrW[1]) + '</br>(' + ifZeroTurn(arrW[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[1]) + '</br>(' + ifZeroTurn(arrD[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[1]) + '</br>(' + ifZeroTurn(arrE[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[1]) + '</br>(' + ifZeroTurn(arrF[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[1]) + '</br>(' + ifZeroTurn(arrT[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[1]) + '</br>(' + ifZeroTurn(arrS[9]) + ')' + '</div>'],
                                    ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrW[2]) + '</br>(' + ifZeroTurn(arrW[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[2]) + '</br>(' + ifZeroTurn(arrD[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[2]) + '</br>(' + ifZeroTurn(arrE[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[2]) + '</br>(' + ifZeroTurn(arrF[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[2]) + '</br>(' + ifZeroTurn(arrT[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[2]) + '</br>(' + ifZeroTurn(arrS[10]) + ')' + '</div>'],
                                    ['累计交易天数(天)', '<div class="align_right">' + ifZeroTurn(arrW[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[15]) + '</div>']
                                ];
                                break;
                            case 'zq':
                                list = [
                                    ['总成交量(万手)', '<div class="align_right">' + ifZeroTurn(arrI[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrJ[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrK[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrL[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrM[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrN[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrO[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrP[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrQ[3]) + '</div>'],
                                    ['最高成交量(万手)', '<div class="align_right">' + ifZeroTurn(arrI[4]) + '<br/>(' + ifZeroTurn(arrI[11]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrJ[4]) + '<br/>(' + ifZeroTurn(arrJ[11]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrK[4]) + '<br/>(' + ifZeroTurn(arrK[11]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrL[4]) + '<br/>(' + ifZeroTurn(arrL[11]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrM[4]) + '<br/>(' + ifZeroTurn(arrM[11]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrN[4]) + '<br/>(' + ifZeroTurn(arrN[11]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrO[4]) + '<br/>(' + ifZeroTurn(arrO[11]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrP[4]) + '<br/>(' + ifZeroTurn(arrP[11]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrQ[4]) + '<br/>(' + ifZeroTurn(arrQ[11]) + ')</div>'],
                                    ['最低成交量(万手)', '<div class="align_right">' + ifZeroTurn(arrI[5]) + '<br/>(' + ifZeroTurn(arrI[12]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrJ[5]) + '<br/>(' + ifZeroTurn(arrJ[12]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrK[5]) + '<br/>(' + ifZeroTurn(arrK[12]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrL[5]) + '<br/>(' + ifZeroTurn(arrL[12]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrM[5]) + '<br/>(' + ifZeroTurn(arrM[12]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrN[5]) + '<br/>(' + ifZeroTurn(arrN[12]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrO[5]) + '<br/>(' + ifZeroTurn(arrO[12]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrP[5]) + '<br/>(' + ifZeroTurn(arrP[12]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrQ[5]) + '<br/>(' + ifZeroTurn(arrQ[12]) + ')</div>'],
                                    ['总成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrI[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrJ[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrK[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrL[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrM[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrN[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrO[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrP[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrQ[6]) + '</div>'],
                                    ['最高成交额(亿元)', '<div class="align_right">' + ifZeroTurn(arrI[7]) + '<br/>(' + ifZeroTurn(arrI[13]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrJ[7]) + '<br/>(' + ifZeroTurn(arrJ[13]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrK[7]) + '<br/>(' + ifZeroTurn(arrK[13]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrL[7]) + '<br/>(' + ifZeroTurn(arrL[13]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrM[7]) + '<br/>(' + ifZeroTurn(arrM[13]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrN[7]) + '<br/>(' + ifZeroTurn(arrN[13]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrO[7]) + '<br/>(' + ifZeroTurn(arrO[13]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrP[7]) + '<br/>(' + ifZeroTurn(arrP[13]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrQ[7]) + '<br/>(' + ifZeroTurn(arrQ[13]) + ')</div>'],
                                    ['最低成交额(亿元)', '<div class="align_right">' + ifZeroTurn(arrI[8]) + '<br/>(' + ifZeroTurn(arrI[14]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrJ[8]) + '<br/>(' + ifZeroTurn(arrJ[14]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrK[8]) + '<br/>(' + ifZeroTurn(arrK[14]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrL[8]) + '<br/>(' + ifZeroTurn(arrL[14]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrM[8]) + '<br/>(' + ifZeroTurn(arrM[14]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrN[8]) + '<br/>(' + ifZeroTurn(arrN[14]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrO[8]) + '<br/>(' + ifZeroTurn(arrO[14]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrP[8]) + '<br/>(' + ifZeroTurn(arrP[14]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrQ[8]) + '<br/>(' + ifZeroTurn(arrQ[14]) + ')</div>'],
                                    ['总成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrI[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrJ[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrK[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrL[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrM[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrN[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrO[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrP[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrQ[0]) + '</div>'],
                                    ['最高笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrI[1]) + '<br/>(' + ifZeroTurn(arrI[9]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrJ[1]) + '<br/>(' + ifZeroTurn(arrJ[9]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrK[1]) + '<br/>(' + ifZeroTurn(arrK[9]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrL[1]) + '<br/>(' + ifZeroTurn(arrL[9]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrM[1]) + '<br/>(' + ifZeroTurn(arrM[9]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrN[1]) + '<br/>(' + ifZeroTurn(arrN[9]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrO[1]) + '<br/>(' + ifZeroTurn(arrO[9]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrP[1]) + '<br/>(' + ifZeroTurn(arrP[9]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrQ[1]) + '<br/>(' + ifZeroTurn(arrQ[9]) + ')</div>'],
                                    ['最低笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrI[2]) + '<br/>(' + ifZeroTurn(arrI[10]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrJ[2]) + '<br/>(' + ifZeroTurn(arrJ[10]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrK[2]) + '<br/>(' + ifZeroTurn(arrK[10]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrL[2]) + '<br/>(' + ifZeroTurn(arrL[10]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrM[2]) + '<br/>(' + ifZeroTurn(arrM[10]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrN[2]) + '<br/>(' + ifZeroTurn(arrN[10]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrO[2]) + '<br/>(' + ifZeroTurn(arrO[10]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrP[2]) + '<br/>(' + ifZeroTurn(arrP[10]) + ')</div>', '<div class="align_right">' + ifZeroTurn(arrQ[2]) + '<br/>(' + ifZeroTurn(arrQ[10]) + ')</div>'],
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
                },
                complete: function() {
                    hideloading();
                }
            });

        }
    }

    //日股票情况
    if ($stockday.length > 0) {
        var init = true;
        var day = '';

        function showajaxStockday() {
            showloading();

            var action = "queryNewTradingByProdTypeData";
            $.ajax({
                url: sseQueryURL + 'marketdata/tradedata/' + action + '.do',
                type: 'post',
                async: false,
                cache: false,
                dataType: "jsonp",
                jsonp: "jsonCallBack",
                jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                data: {
                    searchDate: init ? '' : day,
                    prodType: 'gp'
                },
                success: function(data) {
                    var item = data.result;
                    if (init && item && item[0]) {
                        $("#start_date2").val(item[0].searchDate);
                        $(".sse_table_title2").show().find("p").html('数据日期：' + item[0].searchDate);
                    }

                    var noData = arrayObjNodata(item, ['productType', 'searchDate']);
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
                        tempArr.push("<tr><td colspan='50'>没有数据！</td></tr>");
                    } else {
                        function createArr(item) {
                            var arr = [];
                            var cjbs;
                            arr[0] = item.trdVol; //成交量
                            arr[1] = item.trdAmt; //成交金额
                            if (item.productType == '2') {
                                cjbs = item.trdTm1;
                                if (cjbs != '') {
                                    cjbs = Number(item.trdTm1).toFixed(4);
                                }
                            } else {
                                cjbs = item.trdTm;
                            }

                            arr[2] = cjbs; //成交笔数
                            arr[3] = item.searchDate; //日期
                            arr[4] = item.istVol; //挂牌数
                            arr[5] = item.marketValue; //市价总值
                            arr[6] = item.negotiableValue; //流通市值
                            arr[7] = item.profitRate; //平均市盈率
                            arr[8] = item.exchangeRate; //换手率
                            return arr;
                        }
                        for (var i = 0; i < item.length; i++) {
                            var result = item[i];

                            if (result.productType == "40") {
                                var arrA = createArr(result);
                            } else if (result.productType == "1") {
                                var arrB = createArr(result);
                            } else if (result.productType == "2") {
                                var arrC = createArr(result);
                            } else if (result.productType == "37") {
                                var arrD = createArr(result);
                            } else if (result.productType == "46") {
                                var arrE = createArr(result);
                            } else if (result.productType == "43") {
                                var arrF = createArr(result);
                            } else if (result.productType == "48") {
                                var arrG = createArr(result);
                            }
                        }
                        var list = [
                            ['挂牌数', '<div class="align_right">' + ifundefindTurn(arrA[4]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[4]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[4]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[4]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[4]) + '</div>'],
                            ['市价总值(亿元)', '<div class="align_right">' + ifundefindTurn(arrA[5]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[5]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[5]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[5]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[5]) + '</div>'],
                            ['流通市值(亿元)', '<div class="align_right">' + ifundefindTurn(arrA[6]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[6]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[6]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[6]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[6]) + '</div>'],
                            ['成交金额(亿元)', '<div class="align_right">' + ifundefindTurn(arrA[1]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[1]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[1]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[1]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[1]) + '</div>'],
                            ['成交量(亿股)', '<div class="align_right">' + ifundefindTurn(arrA[0]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[0]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[0]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[0]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[0]) + '</div>'],
                            ['成交笔数(万笔)', '<div class="align_right">' + ifundefindTurn(arrA[2]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[2]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[2]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[2]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[2]) + '</div>'],
                            ['平均市盈率(倍)', '<div class="align_right">' + ifundefindTurn(arrA[7]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[7]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[7]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[7]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[7]) + '</div>'],
                            ['换手率(%)', '<div class="align_right">' + ifundefindTurn(arrA[8]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrB[8]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrC[8]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrG[8]) + '</div>', '<div class="align_right">' + ifundefindTurn(arrF[8]) + '</div>']
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
                },
                complete: function() {
                    hideloading();
                }
            });

        }

        showajaxStockday();
        buttonStockday.on("click", function() {
            init = false;
            day = $("#start_date2").val();
            $(".sse_table_title2").show().find("p").html('数据日期：' + day);
            showajaxStockday();
        });
    }

    //日基金情况
    if ($fundday.length > 0) {
        var init = true;
        var day = '';
        // deschtmlshow('day', '', $('.sse_table_conment'));
        function showajaxFundday() {

            // var sqlid = checkQueryParameObj(queryParameObj, 'rjjqk') ? checkQueryParameObj(queryParameObj, 'rjjqk').sqlid : "COMMON_BOND_SCSJ_SCTJ_TJYB_JYQK_L";
            var action = "queryNewTradingByProdTypeData";
            showloading();
            $.ajax({
                url: sseQueryURL + 'marketdata/tradedata/' + action + '.do',
                type: 'post',
                async: false,
                cache: false,
                dataType: "jsonp",
                jsonp: "jsonCallBack",
                jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                data: {
                    searchDate: init ? '' : day,
                    prodType: 'jj'
                },
                success: function(data) {
                    var result = data.result;
                    if (init && result && result[0]) {
                        $("#start_date2").val(result[0].searchDate);
                        $(".sse_table_title2").show().find("p").html('数据日期：' + result[0].searchDate);
                    }
                    var noData = arrayObjNodata(result, ['productType', 'searchDate']);
                    var header = [
                        ["", "<div class='th_div_center'>单日情况</div>"],
                        ["", "<div class='th_div_center'>基金</div>"],
                        ["", "<div class='th_div_center'>封闭式基金</div>"],
                        ["", "<div class='th_div_center'>ETF</div>"],
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
                            arr[0] = item.trdVol1; //成交量
                            arr[1] = item.trdAmt1; //成交金额
                            arr[2] = item.trdTm1; //成交笔数
                            arr[3] = item.searchDate; //日期
                            arr[4] = item.istVol; //挂牌数
                            return arr;
                        }
                        for (var i = 0; i < result.length; i++) {
                            var item = result[i];

                            if (item.productType == "41") {
                                var arrJj = createArr(item);
                            } else if (item.productType == "3") {
                                var arrFb = createArr(item);
                            } else if (item.productType == "22") {
                                var arrEtf = createArr(item);
                            } else if (item.productType == "35") {
                                var arrLof = createArr(item);
                            } else if (item.productType == "47") {
                                var arrJyhb = createArr(item);
                            } else if (item.productType == "45") {
                                var arrJjhg = createArr(item);
                            }
                        }

                        var list = [
                            ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrJj[4]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrFb[4]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrEtf[4]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrLof[4]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrJyhb[4]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrJjhg[4]) + '</div>'],
                            ['成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrJj[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrFb[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrEtf[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrLof[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrJyhb[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrJjhg[0]) + '</div>'],
                            ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrJj[1]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrFb[1]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrEtf[1]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrLof[1]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrJyhb[1]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrJjhg[1]) + '</div>'],
                            ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrJj[2]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrFb[2]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrEtf[2]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrLof[2]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrJyhb[2]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrJjhg[2]) + '</div>'],
                            // ['', '<div class="align_right">' + ifZeroTurn(item[1].trdVol) + '<br/>(' + ifZeroTurn(item[1].searchDate) + ')</div>', '<div class="align_right">' + ifZeroTurn(item[0].trdVol) + '</div>', '<div class="align_right">' + ifZeroTurn(item[2].trdVol) + '</div>', '<div class="align_right">' + ifZeroTurn(item[3].trdVol) + '</div>', '<div class="align_right">' + ifZeroTurn(item[4].trdVol) + '</div>'],
                            // ['', '<div class="align_right">' + ifZeroTurn(item[1].trdAmt) + '<br/>(' + ifZeroTurn(item[1].searchDate) + ')</div>', '<div class="align_right">' + ifZeroTurn(item[0].trdAmt) + '</div>', '<div class="align_right">' + ifZeroTurn(item[2].trdAmt) + '</div>', '<div class="align_right">' + ifZeroTurn(item[3].trdAmt) + '</div>', '<div class="align_right">' + ifZeroTurn(item[4].trdAmt) + '</div>'],
                            // ['', '<div class="align_right">' + ifZeroTurn(item[1].trdTm) + '<br/>(' + ifZeroTurn(item[1].searchDate) + ')</div>', '<div class="align_right">' + ifZeroTurn(item[0].trdTm) + '</div>', '<div class="align_right">' + ifZeroTurn(item[2].trdTm) + '</div>', '<div class="align_right">' + ifZeroTurn(item[3].trdTm) + '</div>', '<div class="align_right">' + ifZeroTurn(item[4].trdTm) + '</div>']
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
                },
                complete: function() {
                    hideloading();
                }
            });



        }

        showajaxFundday();
        buttonFundday.on("click", function() {
            init = false;
            day = $("#start_date2").val();
            $(".sse_table_title2").show().find("p").html("数据日期：" + day);
            showajaxFundday();
        });
    }

    //日港股通情况
    if ($ggtday.length > 0) {

        $("#start_date2").val(searchDay);

        $(".sse_table_title2").eq(0).show().find("p").html("数据日期：" + searchDay);

        function showajaxGgtday() {

            // var sqlid = checkQueryParameObj(queryParameObj, 'ggtmrcjxx') ? checkQueryParameObj(queryParameObj, 'ggtmrcjxx').sqlid : "COMMON_BOND_SCSJ_SCTJ_TJYB_JYQK_L";
            var action = "getQuatationInfo";
            showloading();
            $.ajax({
                url: sseQueryURL + "ggt/" + action + '.do',
                type: 'post',
                async: false,
                cache: false,
                dataType: "jsonp",
                jsonp: "jsonCallBack",
                jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                data: {
                    tradeDate: day
                },
                async: false,
                cache: false,
                success: function(data) {
                    var item = data.result;
                    var header = [
                        ["", "<div class='th_div_center'>当日情况</div>"],
                        ["", "<div class='th_div_center'>成交概况</div>"]
                    ];
                    var tempArr = [];
                    var headerlength = header.length;
                    tempArr.push("<tr>");
                    for (var j = 0; j < header.length; ++j) {
                        tempArr.push("<th>" + header[j][1] + "</th>");
                    }
                    tempArr.push("</tr>");
                    if (item[0] == "" || item[0] == null || item[0] == undefined || item == undefined) {
                        tempArr.push("<tr><td colspan='50'>没有数据！</td></tr>");

                    } else {

                        var list = [
                            ['当日买入成交金额（亿元）', '<div class="align_right">' + item[0].BUY_AMOUNT + '</div>'],
                            ['当日买入成交笔数（万笔）', '<div class="align_right">' + item[0].BUY_VOLUME + '</div>'],
                            ['当日卖出成交金额（亿元）', '<div class="align_right">' + item[0].SELL_AMOUNT + '</div>'],
                            ['当日卖出成交笔数（万笔）', '<div class="align_right">' + item[0].SELL_VOLUME + '</div>']
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
                    $('.js_tableT01').eq(0).find(".table").html(tempArr.join(""));
                    $(".sse_table_title2").eq(0).show();
                    $(".sse_table_title2").eq(0).find("p").html("数据日期：" + $("#start_date2").val());


                },
                complete: function() {
                    hideloading();
                }

            });



        }



        buttonGgtday.on("click", function() {
            day = $("#start_date2").val().replace(/-/g, '');
            showajaxGgtday();
        });


    }

    //月股票成交概况
    if ($mix.length > 0) {

        //从webservice获取时间
        searchMonth1 = searchMonth.substring(searchMonth.lastIndexOf("-") + 1);
        searchMonth2 = searchMonth.substring(0, 4);
        year = searchMonth2;
        month = searchMonth1;
        var month2 = month;
        if (month != 10) {
            month2 = month.replace(0, '');
        }

        //给下拉框赋值
        $("#single_select_2").find("option").attr("selected", false);
        $("#month_select").find("option[value='" + searchMonth1 + "']").attr("selected", true);
        $("#month_select").next().find("span").html(month2 + "月");
        $("#single_select_2").find("option[value='" + searchMonth2 + "']").attr("selected", true);


        function showajaxMix() {
            $(".sse_table_title2").show().find("p").html("数据日期：" + year + "-" + month);
            showloading();

            var action = "queryMonthlyTradeNew";
            $.ajax({
                url: sseQueryURL + "marketdata/tradedata/" + action + ".do?jsonCallBack=?",
                type: 'post',
                async: false,
                cache: false,
                dataType: "jsonp",
                jsonp: "jsonCallBack",
                jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                data: {
                    prodType: "gp",
                    inYear: searchMonth
                },
                success: function(data) {
                    //var month2 = parseInt(month)-1;
                    //var item = data.result[month2];
                    var result = data.result;
                    var noData = arrayObjNodata(result, ['month', 'mtotalTxDate', 'productType']);
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
                            arr[0] = item.mtotalTx;
                            arr[1] = item.mmaxhighTrn;
                            arr[2] = item.mminLowTrn;
                            arr[3] = item.mtotalVol;
                            arr[4] = item.mmaxTrVol;
                            arr[5] = item.mminTrVol;
                            arr[6] = item.mtotalAmt;
                            arr[7] = item.mmaxTrAmt;
                            arr[8] = item.mminTrAmt;
                            arr[9] = item.mmaxhighTrnDate;
                            arr[10] = item.mminLowTrnDate;
                            arr[11] = item.mmaxTrVolDate;
                            arr[12] = item.mminTrVolDate;
                            arr[13] = item.mmaxTrAmtDate;
                            arr[14] = item.mminTrAmtDate;
                            arr[15] = item.mtotalTxDate;
                            arr[16] = item.mprofitRate;
                            arr[17] = item.mmarketValue;
                            arr[18] = item.mnegotiableValue;
                            arr[19] = item.exchangeRate;
                            arr[20] = item.txNum;
                            return arr;
                        }
                        for (var i = 0; i < result.length; i++) {
                            var item = result[i];

                            if (item.productType == "40") {
                                var arrA = createArr(item);
                            } else if (item.productType == "7") {
                                var arrB = createArr(item);
                            } else if (item.productType == "8") {
                                var arrC = createArr(item);
                            } else if (item.productType == "37") {
                                var arrKcbA = createArr(item);
                            } else if (item.productType == "43") {
                                var arrHg = createArr(item);
                            } else if (item.productType == "46") {
                                var arrKcbC = createArr(item);
                            } else if (item.productType == "48") {
                                var arrU = createArr(item);
                            }
                        }
                        var list = [
                            ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrA[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[20]) + '</div>'],
                            ['市价总值(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[17]) + '</div>'],
                            ['流通市值(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[18]) + '</div>'],
                            ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[6]) + '</div>'],
                            ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[7]) + '</br>(' + ifZeroTurn(arrA[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[7]) + '</br>(' + ifZeroTurn(arrB[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[7]) + '</br>(' + ifZeroTurn(arrC[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[7]) + '</br>(' + ifZeroTurn(arrU[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[7]) + '</br>(' + ifZeroTurn(arrHg[13]) + ')' + '</div>'],
                            ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[8]) + '</br>(' + ifZeroTurn(arrA[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[8]) + '</br>(' + ifZeroTurn(arrB[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[8]) + '</br>(' + ifZeroTurn(arrC[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[8]) + '</br>(' + ifZeroTurn(arrU[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[8]) + '</br>(' + ifZeroTurn(arrHg[14]) + ')' + '</div>'],
                            ['成交量(亿股)', '<div class="align_right">' + ifZeroTurn(arrA[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[3]) + '</div>'],
                            ['最高成交量(亿股)', '<div class="align_right">' + ifZeroTurn(arrA[4]) + '</br>(' + ifZeroTurn(arrA[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[4]) + '</br>(' + ifZeroTurn(arrB[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[4]) + '</br>(' + ifZeroTurn(arrC[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[4]) + '</br>(' + ifZeroTurn(arrU[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[4]) + '</br>(' + ifZeroTurn(arrHg[11]) + ')' + '</div>'],
                            ['最低成交量(亿股)', '<div class="align_right">' + ifZeroTurn(arrA[5]) + '</br>(' + ifZeroTurn(arrA[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[5]) + '</br>(' + ifZeroTurn(arrB[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[5]) + '</br>(' + ifZeroTurn(arrC[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[5]) + '</br>(' + ifZeroTurn(arrU[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[5]) + '</br>(' + ifZeroTurn(arrHg[12]) + ')' + '</div>'],
                            ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[0]) + '</div>'],
                            ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[1]) + '</br>(' + ifZeroTurn(arrA[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[1]) + '</br>(' + ifZeroTurn(arrB[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[1]) + '</br>(' + ifZeroTurn(arrC[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[1]) + '</br>(' + ifZeroTurn(arrU[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[1]) + '</br>(' + ifZeroTurn(arrHg[9]) + ')' + '</div>'],
                            ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[2]) + '</br>(' + ifZeroTurn(arrA[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[2]) + '</br>(' + ifZeroTurn(arrB[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[2]) + '</br>(' + ifZeroTurn(arrC[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[2]) + '</br>(' + ifZeroTurn(arrU[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[2]) + '</br>(' + ifZeroTurn(arrHg[10]) + ')' + '</div>'],
                            ['平均市盈率(倍)', '<div class="align_right">' + ifZeroTurn(arrA[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[16]) + '</div>'],
                            ['换手率(%)', '<div class="align_right">' + ifZeroTurn(arrA[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[19]) + '</div>'],
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
                    //$('.sse_table_conment').find('p').html('*当月数据统计截至前1交易日');
                    deschtmlshow('mon', searchMonth, $('.sse_table_conment'));
                },
                complete: function() {
                    hideloading();
                }
            });

        }
        showajaxMix();

        buttonMix.on("click", function() {
            year = $("#single_select_2").find("option:selected").val();
            month = $("#month_select").find("option:selected").val();
            searchMonth = year + "-" + month;
            showajaxMix();
        });
    }

    //年股票成交概况
    if ($stock.length > 0) {

        //下拉框赋值
        $("#single_select_2").find("option").attr("selected", false);
        $("#single_select_2").find("option[value='" + searchYear + "']").attr("selected", true);

        year = searchYear;

        require(['multipleselect'], function() {
            $('#single_select_2').multipleSelect({
                width: '100%',
                selectAll: false,
                single: true,
                multipleWidth: false,
                maxHeight: 250,
                placeholder: "请选择",
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

        function showajaxStock() {
            $(".sse_table_title2").show().find("p").html("数据日期：" + year + "年");
            showloading();

            var action = "queryNewYearlyTrade";
            $.ajax({
                url: sseQueryURL + "marketdata/tradedata/" + action + ".do?jsonCallBack=?",
                type: 'post',
                async: false,
                cache: false,
                dataType: "jsonp",
                jsonp: "jsonCallBack",
                jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                data: {
                    prodType: "gp",
                    inYear: year
                },
                success: function(data) {

                    //var month2 = parseInt(month)-1;
                    //var item = data.result[month2];
                    var result = data.result;
                    var noData = arrayObjNodata(result, ['year', 'productType']);
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
                            arr[0] = item.ytotalTx;
                            arr[1] = item.ymaxhighTrn;
                            arr[2] = item.yminLowTrn;
                            arr[3] = item.ytotalVol;
                            arr[4] = item.ymaxTrVol;
                            arr[5] = item.yminTrVol;
                            arr[6] = item.ytotalAmt;
                            arr[7] = item.ymaxTrAmt;
                            arr[8] = item.yminTrAmt;
                            arr[9] = item.ymaxhighTrnDate;
                            arr[10] = item.yminLowTrnDate;
                            arr[11] = item.ymaxTrVolDate;
                            arr[12] = item.yminTrVolDate;
                            arr[13] = item.ymaxTrAmtDate;
                            arr[14] = item.yminTrAmtDate;
                            arr[15] = item.ytotalTxDate;
                            arr[16] = item.yprofitRate;
                            arr[17] = item.ymarketValue;
                            arr[18] = item.ynegotiableVale;
                            arr[19] = item.exchangeRate;
                            arr[20] = item.txNum;
                            return arr;
                        }
                        for (var i = 0; i < result.length; i++) {
                            var item = result[i];

                            if (item.productType == "40") {
                                var arrA = createArr(item);
                            } else if (item.productType == "7") {
                                var arrB = createArr(item);
                            } else if (item.productType == "8") {
                                var arrC = createArr(item);
                            } else if (item.productType == "37") {
                                var arrKcbA = createArr(item);
                            } else if (item.productType == "43") {
                                var arrHg = createArr(item);
                            } else if (item.productType == "46") {
                                var arrKcbC = createArr(item);
                            } else if (item.productType == "48") {
                                var arrU = createArr(item);
                            }
                        }
                        var list = [
                            ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrA[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[20]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[20]) + '</div>'],
                            ['市价总值(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[17]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[17]) + '</div>'],
                            ['流通市值(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[18]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[18]) + '</div>'],
                            ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[6]) + '</div>'],
                            ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[7]) + '</br>(' + ifZeroTurn(arrA[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[7]) + '</br>(' + ifZeroTurn(arrB[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[7]) + '</br>(' + ifZeroTurn(arrC[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[7]) + '</br>(' + ifZeroTurn(arrU[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[7]) + '</br>(' + ifZeroTurn(arrHg[13]) + ')' + '</div>'],
                            ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrA[8]) + '</br>(' + ifZeroTurn(arrA[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[8]) + '</br>(' + ifZeroTurn(arrB[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[8]) + '</br>(' + ifZeroTurn(arrC[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[8]) + '</br>(' + ifZeroTurn(arrU[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[8]) + '</br>(' + ifZeroTurn(arrHg[14]) + ')' + '</div>'],
                            ['成交量(亿股)', '<div class="align_right">' + ifZeroTurn(arrA[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[3]) + '</div>'],
                            ['最高成交量(亿股)', '<div class="align_right">' + ifZeroTurn(arrA[4]) + '</br>(' + ifZeroTurn(arrA[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[4]) + '</br>(' + ifZeroTurn(arrB[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[4]) + '</br>(' + ifZeroTurn(arrC[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[4]) + '</br>(' + ifZeroTurn(arrU[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[4]) + '</br>(' + ifZeroTurn(arrHg[11]) + ')' + '</div>'],
                            ['最低成交量(亿股)', '<div class="align_right">' + ifZeroTurn(arrA[5]) + '</br>(' + ifZeroTurn(arrA[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[5]) + '</br>(' + ifZeroTurn(arrB[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[5]) + '</br>(' + ifZeroTurn(arrC[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[5]) + '</br>(' + ifZeroTurn(arrU[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[5]) + '</br>(' + ifZeroTurn(arrHg[12]) + ')' + '</div>'],
                            ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[0]) + '</div>'],
                            ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[1]) + '</br>(' + ifZeroTurn(arrA[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[1]) + '</br>(' + ifZeroTurn(arrB[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[1]) + '</br>(' + ifZeroTurn(arrC[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[1]) + '</br>(' + ifZeroTurn(arrU[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[1]) + '</br>(' + ifZeroTurn(arrHg[9]) + ')' + '</div>'],
                            ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrA[2]) + '</br>(' + ifZeroTurn(arrA[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[2]) + '</br>(' + ifZeroTurn(arrB[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[2]) + '</br>(' + ifZeroTurn(arrC[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[2]) + '</br>(' + ifZeroTurn(arrU[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[2]) + '</br>(' + ifZeroTurn(arrHg[10]) + ')' + '</div>'],
                            ['平均市盈率(倍)', '<div class="align_right">' + ifZeroTurn(arrA[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[16]) + '</div>'],
                            ['换手率(%)', '<div class="align_right">' + ifZeroTurn(arrA[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrU[19]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrHg[19]) + '</div>'],
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
                    //$('.sse_table_conment').show().find('p').html('*当年数据统计截至前1交易日');
                    deschtmlshow('year', year, $('.sse_table_conment'));
                },
                complete: function() {
                    hideloading();
                }
            });

        }

        showajaxStock();

        //查询点击
        buttonStock.on("click", function() {
            year = $("#single_select_2").find("option:selected").val();
            //month = $("#month_select").find("option:selected").val();
            showajaxStock();
        });
    }


    //月基金成交概况
    if ($fundmonth.length > 0) {
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


        function showajaxFundmonth() {

            // var sqlid = checkQueryParameObj(queryParameObj, 'ydjjqk') ? checkQueryParameObj(queryParameObj, 'ydjjqk').sqlid : "COMMON_BOND_SCSJ_SCTJ_TJYB_JYQK_L";
            var action = "queryMonthlyTradeNew";
            $(".sse_table_title2").show().find("p").html("数据日期：" + year + "-" + month);
            showloading();
            $.ajax({
                url: sseQueryURL + 'marketdata/tradedata/' + action + '.do?jsonCallBack=?',
                type: 'post',
                async: false,
                cache: false,
                dataType: "jsonp",
                jsonp: "jsonCallBack",
                jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                data: {
                    prodType: 'jj',
                    inYear: searchMonthJ
                },
                async: false,
                cache: false,
                success: function(data) {

                    var result = data.result;
                    var noData = arrayObjNodata(result, ['month', 'mtotalTxDate', 'productType']);
                    var header = [
                        ["", "<div class='th_div_center'>月度情况</div>"],
                        ["", "<div class='th_div_center'>基金</div>"],
                        ["", "<div class='th_div_center'>封闭式基金</div>"],
                        ["", "<div class='th_div_center'>ETF</div>"],
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
                            arr[0] = item.mtotalTx;
                            arr[1] = item.mmaxhighTrn;
                            arr[2] = item.mminLowTrn;
                            arr[3] = item.mtotalVol;
                            arr[4] = item.mmaxTrVol;
                            arr[5] = item.mminTrVol;
                            arr[6] = item.mtotalAmt;
                            arr[7] = item.mmaxTrAmt;
                            arr[8] = item.mminTrAmt;
                            arr[9] = item.mmaxhighTrnDate;
                            arr[10] = item.mminLowTrnDate;
                            arr[11] = item.mmaxTrVolDate;
                            arr[12] = item.mminTrVolDate;
                            arr[13] = item.mmaxTrAmtDate;
                            arr[14] = item.mminTrAmtDate;
                            arr[15] = item.mtotalTxDate;
                            arr[16] = item.txNum;

                            return arr;
                        }
                        for (var i = 0; i < result.length; i++) {
                            var item = result[i];

                            if (item.productType == "12") {
                                //封闭式基金 12
                                var arrD = createArr(item);
                            } else if (item.productType == "11") {
                                //ETF 11
                                var arrE = createArr(item);
                            } else if (item.productType == "35") {
                                //LOF 35
                                var arrF = createArr(item);
                            } else if (item.productType == "36") {
                                //LOF 36
                                var arrG = createArr(item);
                            } else if (item.productType == "41") {
                                //基金总体 1
                                var arrH = createArr(item);
                            } else if (item.productType == "45") {
                                //基金回购
                                var arrS = createArr(item);
                            } else if (item.productType == "47") {
                                //交易型货币基金
                                var arrT = createArr(item);
                            }
                        }
                        // var list = [

                        //   ['总成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrE[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrA[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[0]) + '</div>'],
                        //   ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrE[1]) + '</br>(' + ifZeroTurn(arrE[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrA[1]) + '</br>(' + ifZeroTurn(arrA[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[1]) + '</br>(' + ifZeroTurn(arrB[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[1]) + '</br>(' + ifZeroTurn(arrC[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[1]) + '</br>(' + ifZeroTurn(arrD[9]) + ')' + '</div>'],
                        //   ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrE[2]) + '</br>(' + ifZeroTurn(arrE[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrA[2]) + '</br>(' + ifZeroTurn(arrA[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[2]) + '</br>(' + ifZeroTurn(arrB[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[2]) + '</br>(' + ifZeroTurn(arrC[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[2]) + '</br>(' + ifZeroTurn(arrD[10]) + ')' + '</div>'],
                        //   ['总成交量(万份)', '<div class="align_right">' + ifZeroTurn(arrE[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrA[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[3]) + '</div>'],
                        //   ['最高成交量(万份)', '<div class="align_right">' + ifZeroTurn(arrE[4]) + '</br>(' + ifZeroTurn(arrE[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrA[4]) + '</br>(' + ifZeroTurn(arrA[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[4]) + '</br>(' + ifZeroTurn(arrB[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[4]) + '</br>(' + ifZeroTurn(arrC[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[4]) + '</br>(' + ifZeroTurn(arrD[11]) + ')' + '</div>'],
                        //   ['最低成交量(万份)', '<div class="align_right">' + ifZeroTurn(arrE[5]) + '</br>(' + ifZeroTurn(arrE[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrA[5]) + '</br>(' + ifZeroTurn(arrA[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[5]) + '</br>(' + ifZeroTurn(arrB[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[5]) + '</br>(' + ifZeroTurn(arrC[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[5]) + '</br>(' + ifZeroTurn(arrD[12]) + ')' + '</div>'],
                        //   ['总成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrE[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrA[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[6]) + '</div>'],
                        //   ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrE[7]) + '</br>(' + ifZeroTurn(arrE[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrA[7]) + '</br>(' + ifZeroTurn(arrA[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[7]) + '</br>(' + ifZeroTurn(arrB[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[7]) + '</br>(' + ifZeroTurn(arrC[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[7]) + '</br>(' + ifZeroTurn(arrD[13]) + ')' + '</div>'],
                        //   ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrE[8]) + '</br>(' + ifZeroTurn(arrE[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrA[8]) + '</br>(' + ifZeroTurn(arrA[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[8]) + '</br>(' + ifZeroTurn(arrB[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[8]) + '</br>(' + ifZeroTurn(arrC[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[8]) + '</br>(' + ifZeroTurn(arrD[14]) + ')' + '</div>'],
                        //   ['累计交易日(天)', '<div class="align_right">' + ifZeroTurn(arrE[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrA[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrB[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrC[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[15]) + '</div>']

                        // ];
                        var list = [
                            ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrH[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[16]) + '</div>'],
                            ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[6]) + '</div>'],
                            ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[7]) + '</br>(' + ifZeroTurn(arrH[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[7]) + '</br>(' + ifZeroTurn(arrD[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[7]) + '</br>(' + ifZeroTurn(arrE[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[7]) + '</br>(' + ifZeroTurn(arrF[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[7]) + '</br>(' + ifZeroTurn(arrT[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[7]) + '</br>(' + ifZeroTurn(arrS[13]) + ')' + '</div>'],
                            ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[8]) + '</br>(' + ifZeroTurn(arrH[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[8]) + '</br>(' + ifZeroTurn(arrD[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[8]) + '</br>(' + ifZeroTurn(arrE[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[8]) + '</br>(' + ifZeroTurn(arrF[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[8]) + '</br>(' + ifZeroTurn(arrT[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[8]) + '</br>(' + ifZeroTurn(arrS[14]) + ')' + '</div>'],
                            ['成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[3]) + '</div>' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[3]) + '</div>'],
                            ['最高成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[4]) + '</br>(' + ifZeroTurn(arrH[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[4]) + '</br>(' + ifZeroTurn(arrD[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[4]) + '</br>(' + ifZeroTurn(arrE[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[4]) + '</br>(' + ifZeroTurn(arrF[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[4]) + '</br>(' + ifZeroTurn(arrT[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[4]) + '</br>(' + ifZeroTurn(arrS[11]) + ')' + '</div>'],
                            ['最低成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[5]) + '</br>(' + ifZeroTurn(arrH[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[5]) + '</br>(' + ifZeroTurn(arrD[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[5]) + '</br>(' + ifZeroTurn(arrE[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[5]) + '</br>(' + ifZeroTurn(arrF[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[5]) + '</br>(' + ifZeroTurn(arrT[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[5]) + '</br>(' + ifZeroTurn(arrS[12]) + ')' + '</div>'],
                            ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[0]) + '</div>'],
                            ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[1]) + '</br>(' + ifZeroTurn(arrH[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[1]) + '</br>(' + ifZeroTurn(arrD[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[1]) + '</br>(' + ifZeroTurn(arrE[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[1]) + '</br>(' + ifZeroTurn(arrF[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[1]) + '</br>(' + ifZeroTurn(arrT[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[1]) + '</br>(' + ifZeroTurn(arrS[9]) + ')' + '</div>'],
                            ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[2]) + '</br>(' + ifZeroTurn(arrH[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[2]) + '</br>(' + ifZeroTurn(arrD[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[2]) + '</br>(' + ifZeroTurn(arrE[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[2]) + '</br>(' + ifZeroTurn(arrF[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[2]) + '</br>(' + ifZeroTurn(arrT[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[2]) + '</br>(' + ifZeroTurn(arrS[10]) + ')' + '</div>'],
                            ['累计交易天数(天)', '<div class="align_right">' + ifZeroTurn(arrH[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[15]) + '</div>']
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
                    //$('.sse_table_conment').show().find('p').html('*当月数据统计截至前1交易日');
                    deschtmlshow('mon', searchMonthJ, $('.sse_table_conment'));
                },
                complete: function() {
                    hideloading();
                }
            });



        }
        //首页ajax加载
        showajaxFundmonth();

        buttonFundmonth.on("click", function() {
            year = $("#single_select_2").find("option:selected").val();
            month = $("#month_select").find("option:selected").val();

            searchMonthJ = year + "-" + month;
            showajaxFundmonth();
        });
    }


    //年基金成交概况
    if ($fundyear.length > 0) {

        $("#single_select_2").find("option").attr("selected", false);
        $("#single_select_2").find("option[value='" + searchYearJ + "']").attr("selected", true);

        year = searchYearJ;

        require(['multipleselect'], function() {
            $('#single_select_2').multipleSelect({
                width: '100%',
                selectAll: false,
                single: true,
                multipleWidth: false,
                maxHeight: 250,
                placeholder: "请选择",
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


        function showajaxFundyear() {

            // var sqlid = checkQueryParameObj(queryParameObj, 'ydzqgk') ? checkQueryParameObj(queryParameObj, 'mrzqgk').sqlid : "COMMON_BOND_SCSJ_SCTJ_TJYB_JYQK_L";
            var action = "queryNewYearlyTrade";
            $(".sse_table_title2").show().find("p").html("数据日期：" + year + "年");
            showloading();
            $.ajax({
                url: sseQueryURL + 'marketdata/tradedata/' + action + '.do?jsonCallBack=?',
                type: 'post',
                async: false,
                cache: false,
                dataType: "jsonp",
                jsonp: "jsonCallBack",
                jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                data: {
                    prodType: 'jj',
                    inYear: year
                },
                success: function(data) {

                    //var month2 = parseInt(month)-1;
                    //var item = data.result[month2];
                    var result = data.result;
                    var noData = arrayObjNodata(result, ['year', 'productType']);
                    var header = [
                        ["", "<div class='th_div_center'>年度情况</div>"],
                        ["", "<div class='th_div_center'>基金</div>"],
                        ["", "<div class='th_div_center'>封闭式基金</div>"],
                        ["", "<div class='th_div_center'>ETF</div>"],
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
                            arr[0] = item.ytotalTx;
                            arr[1] = item.ymaxhighTrn;
                            arr[2] = item.yminLowTrn;
                            arr[3] = item.ytotalVol;
                            arr[4] = item.ymaxTrVol;
                            arr[5] = item.yminTrVol;
                            arr[6] = item.ytotalAmt;
                            arr[7] = item.ymaxTrAmt;
                            arr[8] = item.yminTrAmt;
                            arr[9] = item.ymaxhighTrnDate;
                            arr[10] = item.yminLowTrnDate;
                            arr[11] = item.ymaxTrVolDate;
                            arr[12] = item.yminTrVolDate;
                            arr[13] = item.ymaxTrAmtDate;
                            arr[14] = item.yminTrAmtDate;
                            arr[15] = item.ytotalTxDate;
                            arr[16] = item.txNum;
                            return arr;
                        }
                        for (var i = 0; i < result.length; i++) {
                            var item = result[i];

                            if (item.productType == "12") {
                                //封闭式基金 12
                                var arrD = createArr(item);
                            } else if (item.productType == "11") {
                                //ETF 11
                                var arrE = createArr(item);
                            } else if (item.productType == "35") {
                                //LOF 35
                                var arrF = createArr(item);
                            } else if (item.productType == "36") {
                                //LOF 36
                                var arrG = createArr(item);
                            } else if (item.productType == "41") {
                                //基金总体 1
                                var arrH = createArr(item);
                            } else if (item.productType == "45") {
                                //基金回购
                                var arrS = createArr(item);
                            } else if (item.productType == "47") {
                                //交易型货币基金
                                var arrT = createArr(item);
                            }
                        }

                        var list = [
                            ['挂牌数', '<div class="align_right">' + ifZeroTurn(arrH[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[16]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[16]) + '</div>'],
                            ['成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[6]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[6]) + '</div>'],
                            ['最高成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[7]) + '</br>(' + ifZeroTurn(arrH[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[7]) + '</br>(' + ifZeroTurn(arrD[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[7]) + '</br>(' + ifZeroTurn(arrE[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[7]) + '</br>(' + ifZeroTurn(arrF[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[7]) + '</br>(' + ifZeroTurn(arrT[13]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[7]) + '</br>(' + ifZeroTurn(arrS[13]) + ')' + '</div>'],
                            ['最低成交金额(亿元)', '<div class="align_right">' + ifZeroTurn(arrH[8]) + '</br>(' + ifZeroTurn(arrH[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[8]) + '</br>(' + ifZeroTurn(arrD[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[8]) + '</br>(' + ifZeroTurn(arrE[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[8]) + '</br>(' + ifZeroTurn(arrF[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[8]) + '</br>(' + ifZeroTurn(arrT[14]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[8]) + '</br>(' + ifZeroTurn(arrS[14]) + ')' + '</div>'],
                            ['成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[3]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[3]) + '</div>' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[3]) + '</div>'],
                            ['最高成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[4]) + '</br>(' + ifZeroTurn(arrH[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[4]) + '</br>(' + ifZeroTurn(arrD[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[4]) + '</br>(' + ifZeroTurn(arrE[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[4]) + '</br>(' + ifZeroTurn(arrF[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[4]) + '</br>(' + ifZeroTurn(arrT[11]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[4]) + '</br>(' + ifZeroTurn(arrS[11]) + ')' + '</div>'],
                            ['最低成交量(亿份)', '<div class="align_right">' + ifZeroTurn(arrH[5]) + '</br>(' + ifZeroTurn(arrH[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[5]) + '</br>(' + ifZeroTurn(arrD[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[5]) + '</br>(' + ifZeroTurn(arrE[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[5]) + '</br>(' + ifZeroTurn(arrF[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[5]) + '</br>(' + ifZeroTurn(arrT[12]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[5]) + '</br>(' + ifZeroTurn(arrS[12]) + ')' + '</div>'],
                            ['成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[0]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[0]) + '</div>'],
                            ['最高成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[1]) + '</br>(' + ifZeroTurn(arrH[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[1]) + '</br>(' + ifZeroTurn(arrD[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[1]) + '</br>(' + ifZeroTurn(arrE[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[1]) + '</br>(' + ifZeroTurn(arrF[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[1]) + '</br>(' + ifZeroTurn(arrT[9]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[1]) + '</br>(' + ifZeroTurn(arrS[9]) + ')' + '</div>'],
                            ['最低成交笔数(万笔)', '<div class="align_right">' + ifZeroTurn(arrH[2]) + '</br>(' + ifZeroTurn(arrH[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[2]) + '</br>(' + ifZeroTurn(arrD[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[2]) + '</br>(' + ifZeroTurn(arrE[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[2]) + '</br>(' + ifZeroTurn(arrF[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[2]) + '</br>(' + ifZeroTurn(arrT[10]) + ')' + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[2]) + '</br>(' + ifZeroTurn(arrS[10]) + ')' + '</div>'],
                            ['累计交易天数(天)', '<div class="align_right">' + ifZeroTurn(arrH[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrD[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrE[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrF[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrT[15]) + '</div>', '<div class="align_right">' + ifZeroTurn(arrS[15]) + '</div>']
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
                    //$('.sse_table_conment').show().find('p').html('*当年数据统计截至前1交易日');
                    deschtmlshow('year', year, $('.sse_table_conment'));
                },
                complete: function() {
                    hideloading();
                }
            });


        }

        //首页ajax加载
        showajaxFundyear();

        buttonFundyear.on("click", function() {
            year = $("#single_select_2").find("option:selected").val();
            showajaxFundyear();
        });



    }


    //Q03Q05的检索跳转
    if ($rule.length > 0) {

        //"searchword":'ctitle='+searchword+' and (CRELEASETIME = BETWEEN[2011.07.01,2011.09.01]) and CCHANNELCODE in （xxxx,xxxx）
        var title = "";
        var buttonRule = $rule.find("#btnQuery");
        var $time;
        var ctime;
        var searchword = "";
        var myDate = new Date();
        var year = myDate.getFullYear();
        var day = myDate.getDate();
        var month = 0;
        var today = 0;
        var begindate;
        var enddate;
        var timeend;



        function minusMonth(todayDate, n) {
            var s = todayDate.split(".");
            var yy = parseInt(s[0]);
            var mm = parseInt(s[1]);
            var dd = parseInt(s[2]);
            var dt = new Date(yy, mm, dd);
            var n = dt.setMonth(dt.getMonth() - n);


            if ((dt.getYear() * 12 + dt.getMonth()) < (yy * 12 + mm - n)) {
                dt = new Date(dt.getYear(), dt.getMonth(), 0);
            }
            var nmonth = dt.getMonth();
            var nday = dt.getDate();
            if (nmonth < 10) {
                nmonth = "0" + nmonth;
            }
            if (nday < 10) {
                nday = "0" + nday;
            }
            return dt.getFullYear() + "." + nmonth + "." + nday;
        }

        if ((myDate.getMonth() + 1) < 10) {
            month = myDate.getMonth() + 1
            month = "0" + month
        } else {
            month = myDate.getMonth() + 1
        }
        if (myDate.getDate() < 10) {
            today = '0' + myDate.getDate()
        } else {
            today = myDate.getDate()
        }

        var todayDate = myDate.getFullYear() + "." + month + "." + today;
        var todayDate2 = myDate.getFullYear() + "-" + month + "-" + today;

        begindate = $rule.find("#start_date");
        enddate = $rule.find("#end_date");

        if (begindate != undefined) {

            timeend = minusMonth(todayDate, 3);
            var timeend2 = timeend.replace(/\./g, '-');
            if ($(".doFenLi").length > 0) {

            } else {
                begindate.val(timeend2);
                enddate.val(todayDate2);
            }

        }

        //var col_id=8386;
        //SSE_MENU_28 计算栏目id
        var parentid = "";
        var parent = "";
        var child = "";
        var arr = [];
        //arr.push(col_id);

        function seek(parent) {
            if (SSE_MENU_28[parent].ISSPECIALCHANNEL == '1' && SSE_MENU_28[parent].CHILDREN != "") {
                parentid = parent;
                arr.push(SSE_MENU_28[parent].CHILDREN.replace(/\;/g, ","));
            } else {
                parent = SSE_MENU_28[parent].PARENTCODE;
                arr.push(parent);
                if (parent != "0") {
                    seek(parent);
                }

            }
        }


        if (SSE_MENU_28[col_id].ISSPECIALCHANNEL == '1' && SSE_MENU_28[col_id].CHILDREN != "") {
            parentid = col_id;
            arr.push(SSE_MENU_28[col_id].CHILDREN.replace(/\;/g, ","));
        } else {
            parent = SSE_MENU_28[col_id].PARENTCODE;
            arr.push(parent);
            seek(parent);
        }

        function seek2(child) {
            var childchid = child.split(",");

            for (var i = 0; i < childchid.length; i++) {
                var childchidi = childchid[i];
                if (childchidi != "0" && childchidi != "") {
                    if (SSE_MENU_28[childchidi].CHILDREN != "") {
                        arr.push(SSE_MENU_28[childchidi].CHILDREN.replace(/\;/g, ","));
                    }
                }
            }

        }

        if (arr.length > 1) {
            var childcode = arr[1].split(",");
            for (var i = 0; i < childcode.length; i++) {
                var childcodei = childcode[i];
                if (SSE_MENU_28[childcodei].CHILDREN != "") {
                    arr.push(SSE_MENU_28[childcodei].CHILDREN.replace(/\;/g, ","));
                    child = SSE_MENU_28[childcodei].CHILDREN.replace(/\;/g, ",");
                    seek2(child);
                }
            }
        }

        //日期相减
        buttonRule.on("click", function() {
            //有开始时间和结束时间的页面
            var todayDate2 = myDate.getFullYear() + "-" + month + "-" + today;
            title = $rule.find(".input-group").find("input").val();

            if (title.length > 0) {
                title = title.replace(/[\(\)\[\]\,\，\、\（\）\—\…\/\$\@\=\>\<\!\&\*\%\^\￥\-\#\!\！\+\?\'\\]/g, "");
                title = title.replace(/\\-/g, '`').replace(/-/g, '\\-').replace(/`/g, '\\-');
                title = title.replace(/'/g, '\\\'');
                title = title.replace(/\s+/g, ' ');
                if ($.trim(title) != "") {
                    title = $.trim(title);
                    if (title == "关键字") {
                        alert("请输入检索词！");
                        return false;
                    }
                    //这里得到的search是去除特殊字符和前后空格以及中间部分多余两个以上的的空格，只留一个空格
                } else {
                    alert("输入的检索条件不符合要求，请重新输入！");
                    return false;
                }
            } else {
                alert("请输入检索词！");
                return false;
            }

            var ids = "";
            //ids = arr.join("_");
            ids = arr.join(",");
            //ids = ids.substring(0,ids.length);
            ids = ids.substring(0, (ids.length));


            begindate = $rule.find("#start_date");

            if (begindate.length > 0) {
                if (begindate.val() > enddate.val()) {
                    alert("结束时间不能小于开始时间");
                    return false;
                }
                var end = $("#start_date").val().replace(/\-/g, '.');

                if (title == "") {
                    return false;
                    //searchword = 'searchword=((CRELEASETIME = BETWEEN['+timeend+','+todayDate+']) and CCHANNELCODE in ('+ids+'))';
                    //searchword = 'searchword=0:'+ end +'_'+todayDate + ':' +ids;
                    //webswd=公司,公告&stm=2015.01.01&etm=2015.09.30 
                    //searchword = 'SEARCHWORD =T_L CRELEASETIME T_E BETWEEN['+end+','+todayDate+'] T_R AND CCHANNELCODE IN T_L '+ids+'T_R T_R';
                } else {
                    //searchword = 'searchword='+title+':'+end+'_'+todayDate+':'+ids;
                    //searchword = 'SEARCHWORD =T_L T_L CTITLE T_D CONTENT T_J T_E T_L'+title+'T_R AND T_L CRELEASETIME T_E BETWEEN['+end+','+todayDate+'] T_R AND CCHANNELCODE IN T_L '+ids+'T_R T_R';
                    searchword = 'webswd=' + title + '&stm=' + end + '&etm=' + todayDate + '&chcode=' + ids;
                }
            }


            var select = $rule.find(".single_select");
            if (select != undefined) {
                $time = $rule.find(".single_select").find("option:selected").val();
                if ($time == "") {
                    ctime = "";
                }
                if ($time == "1") {
                    ctime = minusMonth(todayDate, 3);
                }
                if ($time == "2") {
                    ctime = minusMonth(todayDate, 6);

                }
                if ($time == "3") {
                    ctime = minusMonth(todayDate, 12);

                }
                if ($time == "4") {
                    ctime = minusMonth(todayDate, 36);
                }

                if (!begindate.length > 0) {
                    if (ctime == "") {
                        if (title == "") {
                            //searchword = 'searchword=0:'+ids;
                            //searchword = 'SEARCHWORD = CCHANNELCODE IN T_L '+ids+'T_R T_R';
                            return false;
                        } else {
                            //searchword = 'searchword='+title+':'+ids;
                            //searchword = 'SEARCHWORD =T_L T_L CTITLE T_D CONTENT T_JT_E T_L'+title+'T_R  AND CCHANNELCODE IN T_L '+ids+'T_R T_R';
                            searchword = 'webswd=' + title + '&chcode=' + ids;
                        }

                    } else {
                        if (title == "") {
                            //searchword = 'searchword=0:'+ctime+'_'+todayDate+':'+ids;
                            //searchword = 'SEARCHWORD =T_L CRELEASETIME T_E BETWEEN['+ctime+','+todayDate+'] T_R AND CCHANNELCODE IN T_L '+ids+'T_RT_R';
                            return false;
                        } else {
                            //searchword = 'searchword='+title+':'+ctime+'_'+todayDate+':'+ids;
                            //searchword = 'SEARCHWORD =T_L T_L CTITLE T_D CONTENT T_JT_E T_L'+title+'T_R AND T_L CRELEASETIME T_E BETWEEN['+ctime+','+todayDate+'] T_R AND CCHANNELCODE IN T_L '+ids+'T_R T_R';
                            searchword = 'webswd=' + title + '&stm=' + ctime + '&etm=' + todayDate + '&chcode=' + ids;
                        }
                    }
                }

            };
            var gourl = searchUrl + "?" + searchword;

            windowOpen(gourl);
        });
    }



});
$(function() {
    if (!placeholderSupport()) {
        $('[placeholder]').focus(function() {
            var input = $(this);
            if (input.val() == input.attr('placeholder')) {
                input.val('');
                input.removeClass('placeholder');
            }
        }).blur(function() {
            var input = $(this);
            if (input.val() === '' || input.val() == input.attr('placeholder')) {
                input.addClass('placeholderSupport');
                input.val(input.attr('placeholder'));
            }
        }).blur();
    }
});



$(function() {
    /*===============================募集资金首发===========================*/
    var $js_mjzjsf = $(".search_mjzjsf");
    if ($js_mjzjsf.length) {
        var stockTypeDw = '万股'; //0主板A,1主板B,2科创板
        var corder = 0;
        var $tabth = $('.fgrey').parent();
        year(1991, $js_mjzjsf.find('.single_select2').eq(0), '', 1);
        //表头排序查询
        function thclick() {
            for (var itc = 0; itc < $tabth.length; itc++) {
                $tabth.eq(itc).on("click", function() {
                    var ccodher = 0;
                    var thisadd = $(this).find('a').html();
                    var thiszhenf = $(this).find('.glyphicon-arrow-down').length;
                    var syearstemp1 = $('.single_select2').val();
                    var absharetemp1 = $('#tabs-658545').find('.active').find('a').html().replace(/\s+/g, "");
                    //A B股
                    if (absharetemp1 == '主板A') {
                        absharetemp1 = 0;
                    } else if (absharetemp1 == '主板B') {
                        absharetemp1 = 1;
                    } else if (absharetemp1 == '科创板') {
                        absharetemp1 = 2;
                    }
                    //位置
                    if (thisadd.indexOf('证券代码') > -1) {
                        ccodher = 0;
                    } else if (thisadd.indexOf('发行日期') > -1) {
                        ccodher = 2;
                    } else if (thisadd.indexOf('上市日') > -1) {
                        ccodher = 4;
                    }
                    //正反
                    if (thiszhenf == 1) {
                        ccodher++;
                    }
                    searchmjzjsf(syearstemp1, absharetemp1, ccodher);
                });
            }
        }
        //箭头显示部分
        function ordermode(oorder) {
            var jiantou = [
                '<span style="color: #347BB7;" class="glyphicon glyphicon-arrow-down" aria-hidden="true"></span>',
                '<span style="color: #347BB7;" class="glyphicon glyphicon-arrow-up" aria-hidden="true"></span>'
            ];
            $('.fgrey').html('');
            switch (oorder) {
                case 0:
                    $('.fgrey').eq(0).html(jiantou[0]);
                    break;
                case 1:
                    $('.fgrey').eq(0).html(jiantou[1]);
                    break;
                case 2:
                    $('.fgrey').eq(1).html(jiantou[0]);
                    break;
                case 3:
                    $('.fgrey').eq(1).html(jiantou[1]);
                    break;
                case 4:
                    $('.fgrey').eq(2).html(jiantou[0]);
                    break;
                case 5:
                    $('.fgrey').eq(2).html(jiantou[1]);
                    break;
                case 6:
                    break;
                case 7:
                    $('.fgrey').eq(0).html(jiantou[0]);
                    break;
                case 8:
                    $('.fgrey').eq(0).html(jiantou[1]);
                    break;
                case 9:
                    $('.fgrey').eq(1).html(jiantou[0]);
                    break;
                case 10:
                    $('.fgrey').eq(1).html(jiantou[1]);
                    break;
                case 11:
                    $('.fgrey').eq(2).html(jiantou[0]);
                    break;
                case 12:
                    $('.fgrey').eq(2).html(jiantou[1]);
                    break;
                default:
                    break;

            }
        }
        //A股表格
        function showtableAshare(tableData, obj, results, pageCache, calback) {
            var htmlArr1 = [];
            var data = results.dataJson;
            htmlArr1.push('<tr><th><a href="javascript:;">证券代码</a><i class="fgrey"></i></th><th>证券简称</th><th><a href="javascript:;">发行日期</a><i class="fgrey"></i></th><th>发行股数<br/>(' + stockTypeDw + ')</th><th>发行价格</th><th>筹资金额<br/>(万元)</th><th>发行市盈率<br/>(加权法/摊薄法)</th><th>发行方式</th><th>主承销商</th><th>中签率%</th><th><a href="javascript:;">上市日</a><i class="fgrey"></i></th></tr>');
            if (data.length == 0) {
                htmlArr1.push('<tr><td colspan="50">未找到，只有0条数据！</td></tr>');
            } else {
                for (var izs = 0; izs < data.length; izs++) {
                    htmlArr1.push('<tr>');
                    htmlArr1.push('<td><a href="/assortment/stock/list/info/financing/index.shtml?COMPANY_CODE=' + data[izs].COMPANY_CODE + '" target="_blank">' + data[izs].SECURITY_CODE_A + '</a></td>');
                    htmlArr1.push('<td>' + data[izs].SECURITY_NAME_A + '</td>');
                    htmlArr1.push('<td>' + data[izs].BEGIN_DATE + '</td>');
                    htmlArr1.push('<td><div class="align_right">' + tofixed2(data[izs].ISSUED_VOLUME_A) + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].ISSUED_PRICE_A + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + tofixed2(data[izs].RAISED_MONEY_A) + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].ISSUED_PROFIT_RATE_A1 + '/' + data[izs].ISSUED_PROFIT_RATE_A2 + '</div></td>');
                    htmlArr1.push('<td><div class="line-warp">' + data[izs].ISSUED_MODE_CODE_A + '</div></td>');
                    htmlArr1.push('<td><div class="line-warp">' + data[izs].MAIN_UNDERWRITER_NAME_A + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + tofixed2(data[izs].GOT_RATE_A) + '</div></td>');
                    htmlArr1.push('<td>' + data[izs].LISTING_DATE_A + '</td>');
                    htmlArr1.push('</tr>');
                }
            }
            $('.table').html(htmlArr1.join(""));
            tdclickable();
            $tabth = $('.fgrey').parent();
            thclick();
            //箭头显示部分
            ordermode(corder);
            var nowyears = $('.single_select2').val();
            if (nowyears == '') {
                nowyears = todaydata.substring(0, 4);
            }
            $('.sse_table_title2').show().find('p').html('数据日期：' + nowyears + '年');
        }
        //B股表格
        function showtableBshare(tableData, obj, results, pageCache, calback) {
            var htmlArr1 = [];
            var data = results.dataJson;
            htmlArr1.push('<tr><th>证券简称</th><th>发行日期</th><th>发行股数<br/>(万股)</th><th>发行价格<br/>(元人民币/美元)</th><th>筹资金额<br/>(万人民币/万美元)</th><th>发行市盈率<br/>(加权法/摊薄法)</th><th>发行方式</th><th>主承销商</th><th>上市日</th><th>发行公告</th></tr>');
            if (data.length == 0) {
                htmlArr1.push('<tr><td colspan="50">未找到，只有0条数据！</td></tr>');
            } else {
                for (var izs = 0; izs < data.length; izs++) {
                    htmlArr1.push('<tr>');
                    htmlArr1.push('<td><a href="/assortment/stock/list/info/financing/index.shtml?COMPANY_CODE=' + data[izs].COMPANY_CODE + '" target="_blank">' + data[izs].SECURITY_NAME_B + '</a></td>');
                    htmlArr1.push('<td>' + data[izs].BEGIN_DATE + '<br/>至' + data[izs].END_DATE + '</td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].ISSUED_VOLUME_B + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + tofixed2(data[izs].ISSUED_PRICE_B2) + '/' + tofixed2(data[izs].ISSUED_PRICE_B1) + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].RAISED_MONEY_B2 + '/' + data[izs].RAISED_MONEY_B1 + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].ISSUED_PROFIT_RATE_B1 + '/' + data[izs].ISSUED_PROFIT_RATE_B2 + '</div></td>');
                    htmlArr1.push('<td><div class="line-warp">' + data[izs].ISSUED_MODE_CODE_B + '</div></td>');
                    htmlArr1.push('<td><div class="line-warp">' + data[izs].MAIN_UNDERWRITER_NAME_B + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].LISTING_DATE_B + '</div></td>');
                    htmlArr1.push('<td>' + data[izs].ANNOUNCED_DATE + '</td>');
                    htmlArr1.push('</tr>');
                }
            }
            $('.table').html(htmlArr1.join(""));
            tdclickable();
            $tabth = $('.fgrey').parent();
            thclick();
            //箭头显示部分
            ordermode(corder);
            if (data.length == 0) {
                $('.sse_table_title2').hide();
            } else {
                $('.sse_table_title2').show().find('p').html('数据日期：' + data[0].BEGIN_DATE.substring(0, 4) + '年');
            }
        }
        //搜索分类
        function searchmjzjsf(smjsfyear, cshare, order) {
            /*依次 证券代码 发行日期 上市日  升序 降序 A B*/
            var sqlid1 = checkQueryParameObj(queryParameObj, 'sfmjzjqkcodeDESC') ? checkQueryParameObj(queryParameObj, 'sfmjzjqkcodeDESC').sqlid : "COMMON_SSE_GP_SJTJ_MJZJ_SF_AGSF_ZQDMPX_L_NEW_DESC";
            var sqlid2 = checkQueryParameObj(queryParameObj, 'sfmjzjqkcode') ? checkQueryParameObj(queryParameObj, 'sfmjzjqkcode').sqlid : "COMMON_SSE_GP_SJTJ_MJZJ_SF_AGSF_ZQDMPX_L";
            var sqlid3 = checkQueryParameObj(queryParameObj, 'sfmjzjqkrelease') ? checkQueryParameObj(queryParameObj, 'sfmjzjqkrelease').sqlid : "COMMON_SSE_GP_SJTJ_MJZJ_SF_AGSF_FXRPX_L";
            var sqlid4 = checkQueryParameObj(queryParameObj, 'sfmjzjqkreleaseASC') ? checkQueryParameObj(queryParameObj, 'sfmjzjqkreleaseASC').sqlid : "COMMON_SSE_GP_SJTJ_MJZJ_SF_AGSF_FXRPX_L_NEW_ASC";
            var sqlid5 = checkQueryParameObj(queryParameObj, 'sfmjzjqkcome') ? checkQueryParameObj(queryParameObj, 'sfmjzjqkcome').sqlid : "COMMON_SSE_GP_SJTJ_MJZJ_SF_AGSF_SSRPX_L";
            var sqlid6 = checkQueryParameObj(queryParameObj, 'sfmjzjqkcomeASC') ? checkQueryParameObj(queryParameObj, 'sfmjzjqkcomeASC').sqlid : "COMMON_SSE_GP_SJTJ_MJZJ_SF_AGSF_SSRPX_L_NEW_ASC";
            var sqlid7 = checkQueryParameObj(queryParameObj, 'sfmjzjqkB') ? checkQueryParameObj(queryParameObj, 'sfmjzjqkB').sqlid : "COMMON_SSE_GP_SJTJ_MJZJ_SF_BGSF_ZQDM_L_NEW_ASC";
            var sqlIdlist = [
                sqlid1,
                sqlid2,
                sqlid3,
                sqlid4,
                sqlid5,
                sqlid6,
                sqlid7,
                'COMMON_SSE_GP_SJTJ_MJZJ_SF_KCBSF_ZQDMPX_L_NEW_DESC',
                'COMMON_SSE_GP_SJTJ_MJZJ_SF_KCBSF_ZQDMPX_L',
                'COMMON_SSE_GP_SJTJ_MJZJ_SF_KCBSF_FXRPX_L',
                'COMMON_SSE_GP_SJTJ_MJZJ_SF_KCBSF_FXRPX_L_NEW_ASC',
                'COMMON_SSE_GP_SJTJ_MJZJ_SF_KCBSF_SSRPX_L',
                'COMMON_SSE_GP_SJTJ_MJZJ_SF_KCBSF_SSRPX_L_NEW_ASC',
                'COMMON_SSE_GP_SJTJ_MJZJ_SF_BGSF_ZQDM_L_NEW_DESC',
                'COMMON_SSE_GP_SJTJ_MJZJ_SF_BGSF_L',
                'COMMON_SSE_GP_SJTJ_MJZJ_SF_BGSF_L_NEW_ASC',
                'COMMON_SSE_GP_SJTJ_MJZJ_SF_BGSF_SSR_L_NEW_DESC',
                'COMMON_SSE_GP_SJTJ_MJZJ_SF_BGSF_SSR_L_NEW_ASC'
            ];
            if (cshare == 1) {
                order = order + 6;

            } else if (cshare == 2) {
                order = order + 7;

            }
            if (cshare == 2) {
                stockTypeDw = '万股/万份';
            } else {
                stockTypeDw = '万股';
            }
            corder = order;
            var searchmjzjzfdate = {
                isPageing: false,
                url: sseQueryURL + 'commonQuery.do?',
                params: {
                    'isPagination': true,
                    'sqlId': sqlIdlist[order],
                    'pageHelp.pageSize': sitePageSize,
                    'pageHelp.pageNo': nowpage,
                    'pageHelp.beginPage': nowpage,
                    'pageHelp.endPage': 5,
                    'pageHelp.cacheSize': 1
                }
            };
            smjsfyear = smjsfyear ? smjsfyear : currentYear;
            if (smjsfyear > 1990) {
                searchmjzjzfdate.params.searchyear = smjsfyear;
            }
            if (cshare == 0 || cshare == 2) {
                loadPagemjzj(searchmjzjzfdate, {
                    pageSelect: $('.page-con-table').parent()
                }, showtableAshare);
            } else {
                loadPagemjzj(searchmjzjzfdate, {
                    pageSelect: $('.page-con-table').parent()
                }, showtableBshare);
            }
        }
        /*=================首屏加载==========================*/
        searchmjzjsf(currentYear, 0, 0);
        /*==================tab切换=========================*/
        $('#tabs-658545').find('a').eq(0).on("click", function() {
            nowpage = 1;
            searchmjzjsf(currentYear, 0, 0);
            $('.ms-drop').find('li').eq(0).find('input').click();
            $('.ms-drop').find('li').eq(0).find('input').click();
        });
        $('#tabs-658545').find('a').eq(1).on("click", function() {
            nowpage = 1;
            searchmjzjsf(currentYear, 1, 0);
            $('.ms-drop').find('li').eq(0).find('input').click();
            $('.ms-drop').find('li').eq(0).find('input').click();
        });
        $('#tabs-658545').find('a').eq(2).on("click", function() {
            nowpage = 1;
            searchmjzjsf(currentYear, 2, 0);
            $('.ms-drop').find('li').eq(0).find('input').click();
            $('.ms-drop').find('li').eq(0).find('input').click();
        });
        /*=====================按钮查询======================*/
        $('#btnQuery').on("click", function() {
            nowpage = 1;
            var syearstemp = $('.single_select2').val();
            var absharetemp = $('#tabs-658545').find('.active').find('a').html().replace(/\s+/g, "");
            if (absharetemp == '主板A') {
                absharetemp = 0;
            } else if ((absharetemp == '主板B')) {
                absharetemp = 1;
            } else if ((absharetemp == '科创板')) {
                absharetemp = 2;
            }
            searchmjzjsf(syearstemp, absharetemp, 0);
        });

    }

    /*===============================募集资金首发 new ===========================*/
    var $js_mjzjsf_new = $(".search_mjzjsf_new");
    if ($js_mjzjsf_new.length) {
        var jiantou = [
                '<span style="color: #347BB7;" class="glyphicon glyphicon-arrow-down" aria-hidden="true"></span>',
                '<span style="color: #347BB7;" class="glyphicon glyphicon-arrow-up" aria-hidden="true"></span>'
            ],
            _para = [
                ['0', '1,11'],
                [],
                ['1,2', '1,11']
            ],
            nowPage = 1,
            stockTypeDw = '万股',
            ind = 0,
            isDesc = true,
            descandasc = 0,
            jtshow = 0;
        year(1991, $js_mjzjsf_new.find('.single_select2').eq(0), '', 1);

        var searchmjzjzfaandstar = {
            isPageing: false,
            url: sseQueryURL + 'commonQuery.do?',
            params: {
                'isPagination': true,
                'sqlId': 'COMMON_SSE_GP_SJTJ_MJZJ_A_SF_L',
                'issueMark': _para[ind][1],
                'searchYear': todaydata.substring(0, 4),
                'ashareType': _para[ind][0],
                'securityCodeDesc': 1,
                'securityCodeAsc': '',
                'beginDateDesc': '',
                'beginDateAsc': '',
                'listingDateDesc': '',
                'listingDateAsc': '',
                'type': 'inParams',
                'pageHelp.pageSize': sitePageSize,
                'pageHelp.pageNo': nowPage,
                'pageHelp.beginPage': nowPage,
                'pageHelp.endPage': 5,
                'pageHelp.cacheSize': 1
            }
        };

        var searchmjzjzfb = {
            isPageing: false,
            url: sseQueryURL + 'commonQuery.do?',
            params: {
                'isPagination': true,
                'sqlId': 'COMMON_SSE_GP_SJTJ_MJZJ_SF_BGSF_ZQDM_L_NEW_ASC',
                'searchyear': todaydata.substring(0, 4),
                'type': 'inParams',
                'pageHelp.pageSize': sitePageSize,
                'pageHelp.pageNo': nowPage,
                'pageHelp.beginPage': nowPage,
                'pageHelp.endPage': 5,
                'pageHelp.cacheSize': 1
            }
        };

        function orderByDA() {
            $('.js_tableT01 .table tr th a').each(function(i) {
                $(this).on('click', function() {
                    switch (i) {
                        case 0:
                            if (isDesc && descandasc == i) {
                                searchmjzjzfaandstar.params['securityCodeAsc'] = '1';
                                searchmjzjzfaandstar.params['securityCodeDesc'] = '';
                                jtshow = 1;
                                isDesc = false;
                            } else {
                                searchmjzjzfaandstar.params['securityCodeAsc'] = '';
                                searchmjzjzfaandstar.params['securityCodeDesc'] = '1';
                                jtshow = 0;
                                isDesc = true;
                            }
                            searchmjzjzfaandstar.params['beginDateDesc'] = '';
                            searchmjzjzfaandstar.params['beginDateAsc'] = '';
                            searchmjzjzfaandstar.params['listingDateDesc'] = '';
                            searchmjzjzfaandstar.params['listingDateAsc'] = '';
                            break;
                        case 1:
                            if (isDesc && descandasc == i) {
                                searchmjzjzfaandstar.params['beginDateAsc'] = '1';
                                searchmjzjzfaandstar.params['beginDateDesc'] = '';
                                jtshow = 1;
                                isDesc = false;
                            } else {
                                searchmjzjzfaandstar.params['beginDateAsc'] = '';
                                searchmjzjzfaandstar.params['beginDateDesc'] = '1';
                                jtshow = 0;
                                isDesc = true;
                            }
                            searchmjzjzfaandstar.params['securityCodeAsc'] = '';
                            searchmjzjzfaandstar.params['securityCodeDesc'] = '';
                            searchmjzjzfaandstar.params['listingDateDesc'] = '';
                            searchmjzjzfaandstar.params['listingDateAsc'] = '';
                            break;
                        case 2:
                            if (isDesc && descandasc == i) {
                                searchmjzjzfaandstar.params['listingDateAsc'] = '1';
                                searchmjzjzfaandstar.params['listingDateDesc'] = '';
                                jtshow = 1;
                                isDesc = false;
                            } else {
                                searchmjzjzfaandstar.params['listingDateAsc'] = '';
                                searchmjzjzfaandstar.params['listingDateDesc'] = '1';
                                jtshow = 0;
                                isDesc = true;
                            }
                            searchmjzjzfaandstar.params['beginDateDesc'] = '';
                            searchmjzjzfaandstar.params['beginDateAsc'] = '';
                            searchmjzjzfaandstar.params['securityCodeAsc'] = '';
                            searchmjzjzfaandstar.params['securityCodeDesc'] = '';
                            break;
                    }
                    descandasc = i;
                    loadPage(searchmjzjzfaandstar, { pageSelect: $('.js_tableT01') }, showtableAshare);
                })
            })

        }
        //A股、科创板
        function showtableAshare(tableData, obj, results, pageCache, calback) {
            var htmlArr1 = [];
            var data = results.dataJson;
            htmlArr1.push('<tr><th><a href="javascript:;">证券代码</a><i class="fgrey">' + (descandasc == 0 ? jiantou[jtshow] : '') + '</i></th><th>证券简称</th><th><a href="javascript:;">发行日期</a><i class="fgrey">' + (descandasc == 1 ? jiantou[jtshow] : '') + '</i></th><th>发行股数<br/>(' + stockTypeDw + ')</th><th>发行价格</th><th>筹资金额<br/>(万元)</th><th>发行市盈率<br/>(加权法/摊薄法)</th><th>发行方式</th><th>主承销商</th><th>中签率%</th><th><a href="javascript:;">上市日</a><i class="fgrey">' + (descandasc == 2 ? jiantou[jtshow] : '') + '</i></th></tr>');
            if (data && data.length == 0) {
                htmlArr1.push('<tr><td colspan="50">未找到，只有0条数据！</td></tr>');
            } else {
                for (var izs = 0; izs < data.length; izs++) {
                    htmlArr1.push('<tr>');
                    htmlArr1.push('<td><a href="/assortment/stock/list/info/financing/index.shtml?COMPANY_CODE=' + data[izs].COMPANY_CODE + '" target="_blank">' + data[izs].SECURITY_CODE_A + '</a></td>');
                    htmlArr1.push('<td>' + data[izs].SECURITY_NAME_A + '</td>');
                    htmlArr1.push('<td>' + data[izs].BEGIN_DATE + '</td>');
                    htmlArr1.push('<td><div class="align_right">' + tofixed2(data[izs].ISSUED_VOLUME_A) + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].ISSUED_PRICE_A + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + tofixed2(data[izs].RAISED_MONEY_A) + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].ISSUED_PROFIT_RATE_A1 + '/' + data[izs].ISSUED_PROFIT_RATE_A2 + '</div></td>');
                    htmlArr1.push('<td><div class="line-warp">' + data[izs].ISSUED_MODE_CODE_A + '</div></td>');
                    htmlArr1.push('<td><div class="line-warp">' + data[izs].MAIN_UNDERWRITER_NAME_A + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + tofixed2(data[izs].GOT_RATE_A) + '</div></td>');
                    htmlArr1.push('<td>' + data[izs].LISTING_DATE + '</td>');
                    htmlArr1.push('</tr>');
                }
            }
            $('.table').html(htmlArr1.join(""));
            orderByDA();
            var nowyears = $('.single_select2').val();
            if (nowyears == '') {
                nowyears = todaydata.substring(0, 4);
            }
            $('.sse_table_title2').show().find('p').html('数据日期：' + nowyears + '年');
            hideloading();
        }
        loadPage(searchmjzjzfaandstar, { pageSelect: $('.js_tableT01') }, showtableAshare);
        //B股表格
        function showtableBshare(tableData, obj, results, pageCache, calback) {
            var htmlArr1 = [];
            var data = results.dataJson;
            htmlArr1.push('<tr><th>证券简称</th><th>发行日期</th><th>发行股数<br/>(万股)</th><th>发行价格<br/>(元人民币/美元)</th><th>筹资金额<br/>(万人民币/万美元)</th><th>发行市盈率<br/>(加权法/摊薄法)</th><th>发行方式</th><th>主承销商</th><th>上市日</th><th>发行公告</th></tr>');
            if (data.length == 0) {
                htmlArr1.push('<tr><td colspan="50">未找到，只有0条数据！</td></tr>');
            } else {
                for (var izs = 0; izs < data.length; izs++) {
                    htmlArr1.push('<tr>');
                    htmlArr1.push('<td><a href="/assortment/stock/list/info/financing/index.shtml?COMPANY_CODE=' + data[izs].COMPANY_CODE + '" target="_blank">' + data[izs].SECURITY_NAME_B + '</a></td>');
                    htmlArr1.push('<td>' + data[izs].BEGIN_DATE + '<br/>至' + data[izs].END_DATE + '</td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].ISSUED_VOLUME_B + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + tofixed2(data[izs].ISSUED_PRICE_B2) + '/' + tofixed2(data[izs].ISSUED_PRICE_B1) + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].RAISED_MONEY_B2 + '/' + data[izs].RAISED_MONEY_B1 + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].ISSUED_PROFIT_RATE_B1 + '/' + data[izs].ISSUED_PROFIT_RATE_B2 + '</div></td>');
                    htmlArr1.push('<td><div class="line-warp">' + data[izs].ISSUED_MODE_CODE_B + '</div></td>');
                    htmlArr1.push('<td><div class="line-warp">' + data[izs].MAIN_UNDERWRITER_NAME_B + '</div></td>');
                    htmlArr1.push('<td><div class="align_right">' + data[izs].LISTING_DATE_B + '</div></td>');
                    htmlArr1.push('<td>' + data[izs].ANNOUNCED_DATE + '</td>');
                    htmlArr1.push('</tr>');
                }
            }
            $('.table').html(htmlArr1.join(""));
            //箭头显示部分
            if (data.length == 0) {
                $('.sse_table_title2').hide();
            } else {
                $('.sse_table_title2').show().find('p').html('数据日期：' + data[0].BEGIN_DATE.substring(0, 4) + '年');
            }
        }

        function reset() {
            descandasc = 0,
                jtshow = 0;
            isDesc = true;
            searchmjzjzfaandstar.params['securityCodeAsc'] = '';
            searchmjzjzfaandstar.params['securityCodeDesc'] = '1';
            searchmjzjzfaandstar.params['beginDateDesc'] = '';
            searchmjzjzfaandstar.params['beginDateAsc'] = '';
            searchmjzjzfaandstar.params['listingDateDesc'] = '';
            searchmjzjzfaandstar.params['listingDateAsc'] = '';
        }

        $('#tabs-658545 .nav-tabs li').each(function(i, obj) {

            $(this).on('click', function() {
                $('.ms-drop').find('li').eq(0).find('input').click();
                $('.ms-drop').find('li').eq(0).find('input').click();
                searchmjzjzfaandstar.params["pageHelp.pageNo"] = 1;
                searchmjzjzfaandstar.params["pageHelp.beginPage"] = 1;
                var syearstemp = $('.single_select2').val();
                ind = i;
                if (i != 1) {
                    stockTypeDw = (i == 2 ? '万股/万份' : '万股');
                    searchmjzjzfaandstar.params.ashareType = _para[ind][0];
                    searchmjzjzfaandstar.params.issueMark = _para[ind][1];
                    reset();
                    searchmjzjzfaandstar.params.searchYear = syearstemp ? syearstemp : todaydata.substring(0, 4);
                    loadPage(searchmjzjzfaandstar, { pageSelect: $('.js_tableT01') }, showtableAshare);
                } else {
                    searchmjzjzfb.params["pageHelp.pageNo"] = 1;
                    searchmjzjzfb.params["pageHelp.beginPage"] = 1;
                    searchmjzjzfb.params.searchyear = syearstemp ? syearstemp : todaydata.substring(0, 4);
                    loadPage(searchmjzjzfb, { pageSelect: $('.js_tableT01') }, showtableBshare);
                }
            })

        })

        $js_mjzjsf_new.find('#btnQuery').on('click', function() {
            var syearstemp = $('.single_select2').val();
            if (ind != 1) {
                searchmjzjzfaandstar.params["pageHelp.pageNo"] = 1;
                searchmjzjzfaandstar.params["pageHelp.beginPage"] = 1;
                searchmjzjzfaandstar.params.ashareType = _para[ind][0];
                searchmjzjzfaandstar.params.issueMark = _para[ind][1];
                searchmjzjzfaandstar.params.searchYear = syearstemp ? syearstemp : todaydata.substring(0, 4);
                reset();
                loadPage(searchmjzjzfaandstar, { pageSelect: $('.js_tableT01') }, showtableAshare);
            } else {
                searchmjzjzfb.params["pageHelp.pageNo"] = 1;
                searchmjzjzfb.params["pageHelp.beginPage"] = 1;
                searchmjzjzfb.params.searchyear = syearstemp ? syearstemp : todaydata.substring(0, 4);
                loadPage(searchmjzjzfb, { pageSelect: $('.js_tableT01') }, showtableBshare);
            }
        })
    }

    /*===============================募集资金首发 new ===========================*/

    /*====================募集资金增发=======================*/
    var $js_mjzjzf = $(".search_mjzjzf");
    if ($js_mjzjzf.length) {
        year(1999, $js_mjzjzf.find('.single_select2').eq(0), '', 1);
        var stockTypeDw = '万股';

        function showtablemjzjzf(tableData, obj, results, pageCache, calback) {
            var sharetype = calback.sqlId;
            var htmlArr1 = [];
            var data = results.dataJson;
            if (data.length == 0) {
                if (sharetype == 'COMMON_SSE_GP_SJTJ_MJZJ_ZF_AGZF_L' || sharetype == 'COMMON_SSE_GP_SJTJ_MJZJ_ZF_KCBZF_L') {
                    htmlArr1.push('<tr><th>证券简称</th><th>发行日期</th><th>发行股数<br/>(' + stockTypeDw + ')</th><th>发行价格</th><th>筹资金额<br/>(万元)</th><th>发行<br/>市盈率<br/>(加权法/摊薄法)</th><th>发行方式</th><th>主承销商</th><th>中签率<br/>(%)</th><th>上市日</th><th>招股<br/>说明书</th><th>发行公告</th></tr>');
                } else if (sharetype == 'COMMON_SSE_GP_SJTJ_MJZJ_ZF_BGZF_L') {
                    htmlArr1.push('<tr><th>证券简称</th><th>发行<br/>日期</th><th>发行股数<br/>(' + stockTypeDw + ')</th><th>发行价格<br/>(元人民币/美元)</th><th>筹资金额<br/>(万人民币/万美元)</th><th>发行市盈率<br/>(加权法/摊薄法)</th><th>发行方式</th><th>主承销商</th><th>中签<br/>率(%)</th><th>上市日</th><th>招股<br/>说明书</th><th>发行<br/>公告</th></tr>');
                }
                htmlArr1.push('<tr><td colspan="50">未找到，只有0条数据！</td></tr>');
            } else {
                if (sharetype == 'COMMON_SSE_GP_SJTJ_MJZJ_ZF_AGZF_L' || sharetype == 'COMMON_SSE_GP_SJTJ_MJZJ_ZF_KCBZF_L') {
                    htmlArr1.push('<tr><th>证券简称</th><th>发行日期</th><th>发行股数<br/>(' + stockTypeDw + ')</th><th>发行价格</th><th>筹资金额<br/>(万元)</th><th>发行<br/>市盈率<br/>(加权法/摊薄法)</th><th>发行方式</th><th>主承销商</th><th>中签率<br/>(%)</th><th>上市日</th><th>招股<br/>说明书</th><th>发行公告</th></tr>');
                    for (var imjzf = 0; imjzf < data.length; imjzf++) {
                        htmlArr1.push('<tr>');
                        htmlArr1.push('<td><a href="/assortment/stock/list/info/financing/index.shtml?COMPANY_CODE=' + data[imjzf].COMPANY_CODE + '" target="_blank">' + data[imjzf].SECURITY_NAME_A + '</a></td>');
                        htmlArr1.push('<td>' + data[imjzf].BEGIN_DATE + '</td>');
                        htmlArr1.push('<td><div class="align_right">' + tofixed2(data[imjzf].ISSUED_VOLUME_A) + '</td>');
                        htmlArr1.push('<td><div class="align_right">' + data[imjzf].ISSUED_PRICE_A + '</td>');
                        htmlArr1.push('<td><div class="align_right">' + tofixed2(data[imjzf].RAISED_MONEY_A) + '</td>');
                        htmlArr1.push('<td><div class="align_right">' + data[imjzf].ISSUED_PROFIT_RATE_A1 + '/' + data[imjzf].ISSUD_PROFIT_RATE_A2 + '</div></td>');
                        htmlArr1.push('<td><div class="line-warp">' + data[imjzf].ISSUED_MODE_CODE_A + '</div></td>');
                        htmlArr1.push('<td><div class="line-warp">' + data[imjzf].MAIN_UNDERWRITER_NAME_A + '</div></td>');
                        htmlArr1.push('<td><div class="align_right">' + data[imjzf].GOT_RATE_A + '</div></td>');
                        htmlArr1.push('<td>' + data[imjzf].LISTING_DATE_A + '</td>');
                        htmlArr1.push('<td><div class="line-warp">' + data[imjzf].PUBLISHED_DATE + '</div></td>');
                        htmlArr1.push('<td>' + data[imjzf].ANNOUNCED_DATE + '</td>');
                        htmlArr1.push('</tr>');
                    }
                } else {
                    htmlArr1.push('<tr><th>证券简称</th><th>发行<br/>日期</th><th>发行股数<br/>(' + stockTypeDw + ')</th><th>发行价格<br/>(元人民币<br/>/美元)</th><th>筹资金额<br/>(万人民币/万美元)</th><th>发行<br/>市盈率<br/>(加权法/<br/>摊薄法)</th><th>发行方式</th><th>主承销商</th><th>中签<br/>率(%)</th><th>上市日</th><th>招股<br/>说明书</th><th>发行<br/>公告</th></tr>');
                    for (var imjzf = 0; imjzf < data.length; imjzf++) {
                        htmlArr1.push('<tr>');
                        htmlArr1.push('<td><a href="/assortment/stock/list/info/financing/index.shtml?COMPANY_CODE=' + data[imjzf].COMPANY_CODE + '" target="_blank">' + data[imjzf].SECURITY_NAME_B + '</a></td>');
                        htmlArr1.push('<td>-</td>');
                        htmlArr1.push('<td><div class="align_right">' + tofixed2(data[imjzf].ISSUED_VOLUME_B) + '</div></td>');
                        htmlArr1.push('<td><div class="align_right">' + tofixed2(data[imjzf].ISSUED_PRICE_B2) + '/' + tofixed2(data[imjzf].ISSUED_PRICE_B1) + '</div></td>');
                        htmlArr1.push('<td><div class="align_right">' + tofixed2(data[imjzf].RAISED_MONEY_B2) + '/' + tofixed2(data[imjzf].RAISED_MONEY_B1) + '</div></td>');
                        htmlArr1.push('<td><div class="align_right">' + data[imjzf].ISSUED_PROFIT_RATE_B1 + '/' + data[imjzf].ISSUED_PROFIT_RATE_B2 + '</div></td>');
                        htmlArr1.push('<td><div class="line-warp">' + data[imjzf].ISSUED_MODE_CODE_B + '</div></td>');
                        htmlArr1.push('<td><div class="line-warp">' + data[imjzf].MAIN_UNDERWRITER_NAME_B + '</div></td>');
                        htmlArr1.push('<td><div class="align_right">-</div></td>');
                        htmlArr1.push('<td>' + data[imjzf].LISTING_DATE_B + '</td>');
                        htmlArr1.push('<td>-</td>');
                        htmlArr1.push('<td>-</td>');
                        htmlArr1.push('</tr>');
                    }
                }
            }
            $('.table').html(htmlArr1.join(""));
            tdclickable();
            if (data.length == 0) {
                $('.sse_table_title2').hide();
            } else {
                $('.sse_table_title2').show().find('p').html('数据日期：' + data[0].BEGIN_DATE.substring(0, 4) + '年');
            }

        }

        function searchmjzjzf(syears, sharetype) {

            var sqlid1 = checkQueryParameObj(queryParameObj, 'sfmjzjqkA') ? checkQueryParameObj(queryParameObj, 'sfmjzjqkA').sqlid : "COMMON_SSE_GP_SJTJ_MJZJ_ZF_AGZF_L"
            var sqlid2 = checkQueryParameObj(queryParameObj, 'sfmjzjqkB') ? checkQueryParameObj(queryParameObj, 'sfmjzjqkB').sqlid : "COMMON_SSE_GP_SJTJ_MJZJ_ZF_BGZF_L"
            var sqlid3 = 'COMMON_SSE_GP_SJTJ_MJZJ_ZF_KCBZF_L';
            var sqlIdlib = [sqlid1, sqlid2, sqlid3];
            var searchmjzjzfdate = {
                isPageing: false,
                url: sseQueryURL + 'commonQuery.do?',
                params: {
                    'isPagination': true,
                    'sqlId': sqlIdlib[sharetype],
                    'pageHelp.pageSize': sitePageSize,
                    'pageHelp.pageNo': 1,
                    'pageHelp.beginPage': 1,
                    'pageHelp.endPage': 5,
                    'pageHelp.cacheSize': 1
                }
            };
            if (sharetype == 2) {
                stockTypeDw = '万股/万份';
            } else {
                stockTypeDw = '万股';
            }
            if (syears > 1990) {
                searchmjzjzfdate.params.searchyear = syears;
            }
            loadPage(searchmjzjzfdate, {
                pageSelect: $('.page-con-table').parent()
            }, showtablemjzjzf);
        }
        searchmjzjzf(currentYear, 0);
        $('#btnQuery').on("click", function() {
            var syearstemp = $js_mjzjzf.find('.single_select2').val();
            var absharetemp = $('#tabs-658545').find('.active').find('a').html().replace(/\s+/g, "");
            if (absharetemp == '主板A') {
                absharetemp = 0;
            } else if (absharetemp == '主板B') {
                absharetemp = 1;
            } else if (absharetemp == '科创板') {
                absharetemp = 2;
            }
            searchmjzjzf(syearstemp, absharetemp);
        });
        $('#tabs-658545').find('a').eq(0).on("click", function() {
            searchmjzjzf(currentYear, 0);
            $('.ms-drop').find('li').eq(0).find('input').click();
            $('.ms-drop').find('li').eq(0).find('input').click();
        });
        $('#tabs-658545').find('a').eq(1).on("click", function() {
            searchmjzjzf(currentYear, 1);
            $('.ms-drop').find('li').eq(0).find('input').click();
            $('.ms-drop').find('li').eq(0).find('input').click();
        });
        $('#tabs-658545').find('a').eq(2).on("click", function() {
            searchmjzjzf(currentYear, 2);
            $('.ms-drop').find('li').eq(0).find('input').click();
            $('.ms-drop').find('li').eq(0).find('input').click();
        });
    }
    /*=====================募集资金配股======================*/
    var $js_mjzjpg = $(".search_mjzjpg");
    if ($js_mjzjpg.length) {
        year(1997, $js_mjzjpg.find('.single_select2').eq(0), '', 1);
        var stockTypeDw = '万股';

        function showtablemjzjpg(tableData, obj, results, pageCache, calback) {
            var sharetype = calback.sqlId;
            var htmlArr1 = [];
            var data = results.dataJson;
            if (sharetype == 'COMMON_SSE_GP_SJTJ_MJZJ_PG_AGPG_L' || sharetype == 'COMMON_SSE_GP_SJTJ_MJZJ_PG_KCBPG_L') {
                htmlArr1.push('<tr><th>股票简称</th><th>A股股权登记日</th><th>A股除权交易日</th><th>A股<br/>配股价格</th><th>配股比例<br/>(10：?)</th><th>配股缴款起始日</th><th>配股缴款截止日</th><th>实际配股量(' + stockTypeDw + ')</th><th>配股上市日</th></tr>');
            } else {
                htmlArr1.push('<tr><th>股票简称</th><th>B股股权登记日</th><th>B股除权交易日</th><th>B股<br/>配股价格<br/>(美元/汇率)</th><th>配股比例<br/>(10：?)</th><th>配股缴款起始日</th><th>配股缴款截止日</th><th>实际配股量<br/>(万股)</th><th>配股上市日</th></tr>');
            }
            if (data.length == 0) {
                htmlArr1.push('<tr><td colspan="50">未找到，只有0条数据！</td></tr>');
            } else {
                if (sharetype == 'COMMON_SSE_GP_SJTJ_MJZJ_PG_AGPG_L' || sharetype == 'COMMON_SSE_GP_SJTJ_MJZJ_PG_KCBPG_L') {
                    for (var imjpg = 0; imjpg < data.length; imjpg++) {
                        htmlArr1.push('<tr>');
                        htmlArr1.push('<td><a href="/assortment/stock/list/info/financing/index.shtml?COMPANY_CODE=' + data[imjpg].COMPANY_CODE + '" target="_blank">' + data[imjpg].SECURITY_NAME_A + '</a></td>');
                        htmlArr1.push('<td>' + data[imjpg].RECORD_DATE_A + '</td>');
                        htmlArr1.push('<td>' + data[imjpg].EX_RIGHTS_DATE_A + '</td>');
                        htmlArr1.push('<td><div class="align_right">' + tofixed2(data[imjpg].PRICE_OF_RIGHTS_ISSUE_A) + '</div></td>');
                        htmlArr1.push('<td><div class="align_right">' + tofixed2(data[imjpg].RATIO_OF_RIGHTS_ISSUE_A) + '</div></td>');
                        htmlArr1.push('<td>' + data[imjpg].START_DATE_OF_REMITTANCE_A + '</td>');
                        htmlArr1.push('<td>' + data[imjpg].END_DATE_OF_REMITTANCE_A + '</td>');
                        htmlArr1.push('<td><div class="align_right">' + tofixed2(data[imjpg].TRUE_COLUME_A) + '</div></td>');
                        htmlArr1.push('<td>' + data[imjpg].LISTING_DATE_A + '</td>');
                        htmlArr1.push('</tr>');
                    }
                } else {
                    for (var imjpg = 0; imjpg < data.length; imjpg++) {
                        htmlArr1.push('<tr>');
                        htmlArr1.push('<td><a href="/assortment/stock/list/info/financing/index.shtml?COMPANY_CODE=' + data[imjpg].COMPANY_CODE + '" target="_blank">' + data[imjpg].SECURITY_NAME_B + '</a></td>');
                        htmlArr1.push('<td>' + data[imjpg].RECORD_DATE_B + '</td>');
                        htmlArr1.push('<td>' + data[imjpg].EX_RIGHTS_DATE_B + '</td>');
                        htmlArr1.push('<td><div class="align_right">' + tofixed2(data[imjpg].PRICE_OF_RIGHTS_ISSUE_B) + '/' + tofixed2(data[imjpg].EXCHANGE_RATE) + '</div></td>');
                        htmlArr1.push('<td><div class="align_right">' + tofixed2(data[imjpg].RATIO_OF_RIGHTS_ISSUE_B) + '</div></td>');
                        htmlArr1.push('<td>' + data[imjpg].START_DATE_OF_REMITTANCE_B + '</td>');
                        htmlArr1.push('<td>' + data[imjpg].END_DATE_OF_REMITTANCE_B + '</td>');
                        htmlArr1.push('<td><div class="align_right">' + tofixed2(data[imjpg].TRUE_COLUME_B) + '</div></td>');
                        htmlArr1.push('<td>' + data[imjpg].LISTING_DATE_B + '</td>');
                        htmlArr1.push('</tr>');
                    }
                }
            }
            $('.table').html(htmlArr1.join(""));
            tdclickable();
            if (data.length == 0) {
                $('.sse_table_title2').hide();
            } else {
                if (sharetype == 'COMMON_SSE_GP_SJTJ_MJZJ_PG_AGPG_L' || sharetype == 'COMMON_SSE_GP_SJTJ_MJZJ_PG_KCBPG_L') {
                    $('.sse_table_title2').show().find('p').html('数据日期：' + data[0].LISTING_DATE_A.substring(0, 4) + '年');
                } else {
                    $('.sse_table_title2').show().find('p').html('数据日期：' + data[0].LISTING_DATE_B.substring(0, 4) + '年');
                }
            }
        }

        function searchmjzjpg(syears, sharetype) {
            var sqlid1 = checkQueryParameObj(queryParameObj, 'pgmjzjqkA') ? checkQueryParameObj(queryParameObj, 'pgmjzjqkA').sqlid : "COMMON_SSE_GP_SJTJ_MJZJ_PG_AGPG_L"
            var sqlid2 = checkQueryParameObj(queryParameObj, 'pgmjzjqkB') ? checkQueryParameObj(queryParameObj, 'pgmjzjqkB').sqlid : "COMMON_SSE_GP_SJTJ_MJZJ_PG_BGPG_L"
            var sqlid3 = 'COMMON_SSE_GP_SJTJ_MJZJ_PG_KCBPG_L';
            var sqlIdlib = [sqlid1, sqlid2, sqlid3];
            var searchmjzjzfdate = {
                isPageing: false,
                url: sseQueryURL + 'commonQuery.do?',
                params: {
                    'isPagination': true,
                    'sqlId': sqlIdlib[sharetype],
                    'pageHelp.pageSize': sitePageSize,
                    'pageHelp.pageNo': 1,
                    'pageHelp.beginPage': 1,
                    'pageHelp.endPage': 5,
                    'pageHelp.cacheSize': 1
                }
            };
            if (sharetype == 2) {
                stockTypeDw = '万股/万份';
            } else {
                stockTypeDw = '万股';
            }
            if (syears > 1990) {
                searchmjzjzfdate.params.searchyear = syears;
            }
            loadPage(searchmjzjzfdate, {
                pageSelect: $('.page-con-table').parent()
            }, showtablemjzjpg);
        }
        searchmjzjpg(currentYear, 0);
        $('#tabs-658545').find('a').eq(0).on("click", function() {
            searchmjzjpg(currentYear, 0);
            $('.ms-drop').find('li').eq(0).find('input').click();
            $('.ms-drop').find('li').eq(0).find('input').click();
        });
        $('#tabs-658545').find('a').eq(1).on("click", function() {
            searchmjzjpg(currentYear, 1);
            $('.ms-drop').find('li').eq(0).find('input').click();
            $('.ms-drop').find('li').eq(0).find('input').click();
        });
        $('#tabs-658545').find('a').eq(2).on("click", function() {
            searchmjzjpg(currentYear, 2);
            $('.ms-drop').find('li').eq(0).find('input').click();
            $('.ms-drop').find('li').eq(0).find('input').click();
        });
        $('#btnQuery').on("click", function() {
            var syearstemp = $js_mjzjpg.find('.single_select2').val();
            var absharetemp = $('#tabs-658545').find('.active').find('a').html().replace(/\s+/g, "");
            if (absharetemp == '主板A') {
                absharetemp = 0;
            } else if (absharetemp == '主板B') {
                absharetemp = 1;
            } else if (absharetemp == '科创板') {
                absharetemp = 2;
            }
            searchmjzjpg(syearstemp, absharetemp);
        });
    }
});



//======================================活跃股排名=============================================
var $jssearchHygpm = $(".search_hygpm_kcb");
if ($jssearchHygpm.length > 0) {

    var $sse_table_title2_p = $(".sse_table_title2");
    var $qbuttonHygpm = $jssearchHygpm.find(".btn-primary");
    showdate = showdate.substring(0, 10);
    var tabli = document.getElementById('tabs-658545').getElementsByTagName('li');

    function showajaxHygpm(jsonObj) {
        // console.log(jsonObj);
        showloading();
        $.ajax({
            url: jsonObj.url,
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: jsonObj.param,

            success: function(callBackData) {
                doShowAjaxHygpm(callBackData);
            },
            complete: function() {
                hideloading();
            }
        });
    }

    for (var ipd1 = 0; ipd1 < 6; ipd1++) {
        (function(rankCondition) {
            tabli[ipd1].onclick = function() {
                var condition = rankCondition + 1;
                var start_date = $('#start_date').val();
                var end_date = $('#end_date').val();
                if (start_date == undefined || start_date == "开始日期" || start_date == null || start_date == "") {
                    start_date = "";
                }
                if (end_date == undefined || end_date == "结束日期" || end_date == null || end_date == "") {
                    end_date = "";
                }
                if (end_date != '' && start_date != '') {
                    var action = 'queryKCBTopByPage';
                    showajaxHygpm({
                        url: sseQueryURL + 'marketdata/tradedata/' + action + '.do',
                        param: {
                            'startDate': start_date,
                            'endDate': end_date,
                            'rankCondition': condition,
                            'isPagination': true,
                            'pageHelp.pageSize': 20,
                            'pageHelp.pageNo': 1,
                            'pageHelp.beginPage': 1,
                            'pageHelp.cacheSize': 1,
                            'pageHelp.endPage': 5
                        }
                    });
                }
            };
        })(ipd1);
    }

    /*
     * 数据展示
     */
    function doShowAjaxHygpm(callBackData) {
        var tableArr = [];
        var tabnum = parseInt(callBackData.rankCondition) - 1;
        tableArr.push('<tr class="greybg">        ');
        tableArr.push(' <th>名<br/>次</th>         ');
        tableArr.push(' <th>股票<br/>代码</th>         ');
        tableArr.push(' <th>股票<br/>简称</th>         ');
        tableArr.push(' <th>开<br/>盘<br/>价</th> ');
        tableArr.push(' <th>收<br/>盘<br/>价</th>');
        tableArr.push(' <th>平<br/>均<br/>价</th>');
        tableArr.push(' <th>累计<br/>成交量<br/>(万股/万份)</th>');
        tableArr.push(' <th>累计成<br/>交金额<br/>(万元)</th>');
        tableArr.push(' <th>累计<br/>涨跌<br/>幅(%)</th>');
        tableArr.push(' <th>期间<br/>振幅<br/>(%)</th>');
        tableArr.push(' <th>累计<br/>换手<br/>率%</th>');
        tableArr.push('</tr>                      ');

        if (callBackData.result.length == 0) {
            tableArr.push('<tr><td colSpan="50">没有查询到数据</td></tr>');
        } else {
            for (var i = 0; i < callBackData.result.length; i++) {
                tableArr.push('<tr>');
                tableArr.push('<td>' + callBackData.result[i].rank + '</td>');
                tableArr.push('<td><a target="_blank" href="/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=' + callBackData.result[i].product + '"> ' + callBackData.result[i].product + '</a></td>');
                tableArr.push('<td>' + callBackData.result[i].productName + '</td>');
                if (callBackData.result[i].openPrice == null || callBackData.result[i].openPrice == undefined) {
                    tableArr.push('<td><div class="align_right">' + '-' + '</div></td>');
                } else {
                    tableArr.push('<td><div class="align_right">' + callBackData.result[i].openPrice + '</div></td>');
                }
                if (callBackData.result[i].closePrice == null || callBackData.result[i].closePrice == undefined) {
                    tableArr.push('<td><div class="align_right">' + '-' + '</div></td>');
                } else {
                    tableArr.push('<td><div class="align_right">' + callBackData.result[i].closePrice + '</div></td>');
                }
                if (callBackData.result[i].evgPrice == null || callBackData.result[i].evgPrice == undefined) {
                    tableArr.push('<td><div class="align_right">' + '-' + '</div></td>');
                } else {
                    tableArr.push('<td><div class="align_right">' + callBackData.result[i].evgPrice + '</div></td>');
                }
                tableArr.push('<td>' + callBackData.result[i].totalVol + '</td>');
                tableArr.push('<td><div class="align_right">' + callBackData.result[i].totalAmt + '</div></td>');
                tableArr.push('<td>' + callBackData.result[i].totalChng + '</td>');
                tableArr.push('<td>' + callBackData.result[i].totalChanges + '</td>');
                tableArr.push('<td>' + callBackData.result[i].totalExchrate + '</td>');
                tableArr.push('</tr>');
            }
        }
        $(".table").eq(tabnum).html(tableArr.join(""));
        tdclickable();
    }

    //按钮点击事件绑定
    $qbuttonHygpm.on("click", function() {
        var start_date = $jssearchHygpm.find("#start_date").val();
        var end_date = $jssearchHygpm.find("#end_date").val();
        if (start_date == undefined || start_date == "开始日期" || start_date == null || start_date == "") {
            start_date = "";
        }
        if (end_date == undefined || end_date == "结束日期" || end_date == null || end_date == "") {
            end_date = "";
        }
        if (start_date == "" || end_date == "") {
            alert("请输入完整日期后查询");
        } else if (start_date > end_date) {
            alert("开始日期不能大于结束日期");
        } else if (start_date < "1990-12-19") {
            alert("您选择的日期超出检索范围，请重输。");
        } else if (end_date > get_systemDate_global()) {
            alert("您选择的日期超出检索范围，请重输。");
        } else {
            var tempData = "数据日期：" + start_date + "至" + end_date;
            $sse_table_title2_p.show().find("p").html(tempData);
            var conditionnow = $('#tabs-658545').find('.active').find('a').html().replace(/\s+/g, "");
            conditionnow = conditionnow == '成交量' ? 1 : conditionnow == '成交金额' ? 2 : conditionnow == '涨幅' ? 3 : conditionnow == '跌幅' ? 4 : conditionnow == '振幅' ? 5 : conditionnow == '换手率' ? 6 : '';
            //ajax传参
            showajaxHygpm({
                url: sseQueryURL + 'marketdata/tradedata/queryKCBTopByPage.do',
                param: {
                    'startDate': start_date,
                    'endDate': end_date,
                    'rankCondition': conditionnow,
                    'isPagination': true,
                    'pageHelp.pageSize': 20,
                    'pageHelp.pageNo': 1,
                    'pageHelp.beginPage': 1,
                    'pageHelp.cacheSize': 1,
                    'pageHelp.endPage': 5
                }
            });
        }
    });
}


//募集资金概况
var $jssearch_placement = $(".search_placement");
if ($jssearch_placement.length) {


    function renumKcb(num, s) {
        s = s ? s : 0;
        var result;
        if (num == '' || num == null || num == undefined || num == 0) {
            result = '-';
            return result;
        }
        result = Number(num).toFixed(s);
        return result;
    }
    var datatime = todaydata;
    datatime = shijian(datatime, 0, 1, 0);
    datatime = datatime.substring(0, 4) + '年' + datatime.substring(5, 7) + '月99日';
    datatime = shijian(datatime, 0, 0, 0);
    var xxx = parseInt(datatime.substring(5, 7) - 1);
    year(2001, $jssearch_placement.find('.single_select2').eq(0), parseInt(datatime.substring(0, 4)));
    datatime = '截止日期：' + datatime.substring(0, 4) + '年' + datatime.substring(5, 7) + '月' + datatime.substring(8, 10) + '日';
    $('.sse_table_title2').find('p').html(datatime);
    $('.sse_table_title2').show();
    // $('.sse_table_conment').find('p').html('注：权证行权筹资额＝股本变动量×最新行权价格；B股筹资金额按当月月底的汇率折算成人民币。公司债=企业债+公司债+可分离债');
    var tablist = [];
    tablist.push('<thead><tr><th rowspan="3" class="text-center" ><p style="width:20px;margin: 0 auto;line-height: 16px;">证券类型</p></th><th rowspan="3" colspan="2" class="text-center"><b>筹、融资方式</b></th><th colspan="4" class="text-center"><b>公司数</b></th>\
  <th colspan="4" class="text-center"><b>筹融资(亿元)</b></th></tr>');
    tablist.push('<tr><th colspan="2" class="text-center"><em>本月</em></th><th colspan="2" class="text-center"><em>本年</em></th><th colspan="2" class="text-center"><em>本月</em></th><th colspan="2" class="text-center"><em>本年</em></th></tr>');
    tablist.push('<tr><th class="text-center"><em>主板</em></th><th class="text-center"><em>科创板</em></th><th class="text-center"><em>主板</em></th><th class="text-center"><em>科创板</em></th><th class="text-center"><em>主板</em></th><th class="text-center"><em>科创板</em></th><th class="text-center"><em>主板</em></th><th class="text-center"><em>科创板</em></th></tr></thead>');
    tablist.push('<tbody>');
    tablist.push('<tr><td style="vertical-align: middle;" rowspan="9"><p style="width:20px;margin: 0 auto;line-height: 16px;font-weight: bold;">普通股</p></td><td style="vertical-align: middle;" rowspan="3"><em>首发</em></td><td><em>公开发行</em></td>\
    <td class="text-center"><em>' + renum(tabledata1.gp_scfx_gkfx[0]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_gkfx[1]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_gkfx[2]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_gkfx[3]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_gkfx[4], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_gkfx[5], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_gkfx[6], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_gkfx[7], 2) + '</em></td></tr>');
    tablist.push('<tr><td><em>超额配售</em></td>\
    <td class="text-center"><em>' + renum(tabledata1.gp_scfx_cepg[0]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_cepg[1]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_cepg[2]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_cepg[3]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_cepg[4], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_cepg[5], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_cepg[6], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_scfx_cepg[7], 2) + '</em></td></tr>');
    tablist.push('<tr class="level_2"><td><em>首发小计</em></td>\
    <td colspan="2" class="text-center"><em>' + renum(tabledata1.gp_scfx_scfxxj[0]) + '</em></td><td colspan="2" class="text-center"><em>' + renum(tabledata1.gp_scfx_scfxxj[1]) + '</em></td><td colspan="2" class="text-center"><em>' + renum(tabledata1.gp_scfx_scfxxj[2], 2) + '</em></td><td colspan="2" class="text-center"><em>' + renum(tabledata1.gp_scfx_scfxxj[3], 2) + '</em></td></tr>');
    tablist.push('<tr><td style="vertical-align: middle;" rowspan="5"><em>再发</em></td><td><em>公开增发</em></td>\
    <td class="text-center"><em>' + renum(tabledata1.gp_zcfx_xgzzf[0]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_xgzzf[1]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_xgzzf[2]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_xgzzf[3]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_xgzzf[4], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_xgzzf[5], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_xgzzf[6], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_xgzzf[7], 2) + '</em></td></tr>');
    tablist.push('<tr><td><em>定向增发</em></td>\
    <td class="text-center"><em>' + renum(tabledata1.gp_zcfx_dxzf[0]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_dxzf[1]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_dxzf[2]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_dxzf[3]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_dxzf[4], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_dxzf[5], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_dxzf[6], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_dxzf[7], 2) + '</em></td></tr>');
    tablist.push('<tr><td><em>配股</em></td>\
    <td class="text-center"><em>' + renum(tabledata1.gp_zcfx_pg[0]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_pg[1]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_pg[2]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_pg[3]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_pg[4], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_pg[5], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_pg[6], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_pg[7], 2) + '</em></td></tr>');
    tablist.push('<tr><td><em>转债转股</em></td>\
    <td class="text-center"><em>' + renum(tabledata1.gp_zcfx_kzhzqzg[0]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_kzhzqzg[1]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_kzhzqzg[2]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_kzhzqzg[3]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_kzhzqzg[4], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_kzhzqzg[5], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_kzhzqzg[6], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gp_zcfx_kzhzqzg[7], 2) + '</em></td></tr>');
    tablist.push('<tr class="level_2"><td><em>再发小计</em></td>\
    <td colspan="2" class="text-center"><em>' + renum(tabledata1.gp_scfx_zcfxxj[0]) + '</em></td><td colspan="2" class="text-center"><em>' + renum(tabledata1.gp_scfx_zcfxxj[1]) + '</em></td><td colspan="2" class="text-center"><em>' + renum(tabledata1.gp_scfx_zcfxxj[2], 2) + '</em></td><td colspan="2" class="text-center"><em>' + renum(tabledata1.gp_scfx_zcfxxj[3], 2) + '</em></td></tr>');
    tablist.push('<tr class="level_1"><td colspan="2"><em>普通股合计</em></td>\
    <td colspan="2" class="text-center"><em>' + renum(tabledata1.gpczhj[0]) + '</em></td><td colspan="2" class="text-center"><em>' + renum(tabledata1.gpczhj[1]) + '</em></td><td colspan="2" class="text-center"><em>' + renum(tabledata1.gpczhj[2], 2) + '</em></td><td colspan="2" class="text-center"><em>' + renum(tabledata1.gpczhj[3], 2) + '</em></td></tr>');
    tablist.push('<tr><td style="vertical-align: middle;" rowspan="3"><p style="width:20px;margin: 0 auto;line-height: 16px;font-weight: bold;">优先股</p></td><td colspan="2"><em>首次发行</em></td>\
    <td class="text-center"><em>' + renum(tabledata1.yxg_scfx[0]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxg_scfx[1]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.yxg_scfx[2]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxg_scfx[3]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.yxg_scfx[4]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxg_scfx[5]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.yxg_scfx[6]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxg_scfx[7]) + '</em></td></tr>');
    tablist.push('<tr><td colspan="2"><em>再次发行</em></td>\
    <td class="text-center"><em>' + renum(tabledata1.yxg_zcfx[0]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxg_zcfx[1]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.yxg_zcfx[2]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxg_zcfx[3]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.yxg_zcfx[4]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxg_zcfx[5]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.yxg_zcfx[6]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxg_zcfx[7]) + '</em></td></tr>');

    tablist.push('<tr class="level_1"><td colspan="2"><em>优先股合计</em></td>\
    <td class="text-center"><em>' + renum(tabledata1.yxgczhj[0]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxgczhj[1]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.yxgczhj[2]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxgczhj[3]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.yxgczhj[4]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxgczhj[5]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.yxgczhj[6]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.yxgczhj[7]) + '</em></td></tr>');
    tablist.push('<tr class="level_1"><td colspan="3"><em>公司债融资</em></td>\
    <td class="text-center"><em>' + renum(tabledata1.gszczhj[0]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.gszczhj[1]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gszczhj[2]) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.gszczhj[3]) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gszczhj[4], 2) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.gszczhj[5], 2) + '</em></td><td class="text-center"><em>' + renum(tabledata1.gszczhj[6], 2) + '</em></td><td class="text-center"><em>' + renumKcb(tabledata1.gszczhj[7], 2) + '</em></td></tr>');

    // tablist.push('<tr class="level_1" ><td><em>市场筹资合计 </em></td><td><i>' + irenum(tabledata1.sctchj[0]) + '</i></td><td><i>' + irenum(tabledata1.sctchj[1]) + '</i></td><td><i>' + irenum(tabledata1.sctchj[2]) + '</i></td><td><i>' + irenum(tabledata1.sctchj[3]) + '</i></td></tr>');
    tablist.push('</tbody>');
    $('.table').html(tablist.join(""));
    $('.sse_table_T03 .table thead tr th b,.sse_table_T03 .table tbody tr.level_2 td em,.sse_table_T03 .table tbody tr.level_1 td em').css('padding-left', '0');
    $('.sse_table_T03 .table thead tr th em').css('padding-right', '0');
    $('.table.b1sd>tbody>tr>td').css('text-align', 'center');

    // $('.table td').css('border','1px solid #ddd');
    $jssearch_placement.find('#btnQuery').on("click", function() {
        var monthyype = $jssearch_placement.find('#month_select').val();
        monthyype = monthyype == null ? xxx + 1 : monthyype;
        var searchdatetemp = $jssearch_placement.find('.single_select2').val() + '-' + monthyype;
        var daySysDate = get_systemDate_global().substring(0, 7);

        /**
         * Li.chen  2016-03-25 17:09:18
         * @param  {[type]} !$jssearch_placement.find('#month_select').val() [description]
         * @return {[type]}                                                  [description]
         */
        if (!$jssearch_placement.find('#month_select').val()) {
            alert("请选择正确的日期！");
            return;
        };
        if (daySysDate < searchdatetemp) {
            alert("您选择的日期超出检索范围，请重输。");
            return;
        };
        var action = "queryNewFundsRaisedProfile";
        var ajaxdata = {
            isPageing: true,
            url: sseQueryURL + 'security/stock/' + action + '.do?',
            params: {
                'searchDate': searchdatetemp
            }
        }
        $.ajax({
            url: ajaxdata.url,
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            data: ajaxdata.params,
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            success: function(callBackData) {
                var jsondata = callBackData.result[0];
                tablist = [];
                tablist.push('<thead><tr><th rowspan="3" class="text-center" ><p style="width:20px;margin: 0 auto;line-height: 16px;">证券类型</p></th><th rowspan="3" colspan="2" class="text-center"><b>筹、融资方式</b></th><th colspan="4" class="text-center"><b>公司数</b></th>\
        <th colspan="4" class="text-center"><b>筹融资(亿元)</b></th></tr>');
                tablist.push('<tr><th colspan="2" class="text-center"><em>本月</em></th><th colspan="2" class="text-center"><em>本年</em></th><th colspan="2" class="text-center"><em>本月</em></th><th colspan="2" class="text-center"><em>本年</em></th></tr>');
                tablist.push('<tr><th class="text-center"><em>主板</em></th><th class="text-center"><em>科创板</em></th><th class="text-center"><em>主板</em></th><th class="text-center"><em>科创板</em></th><th class="text-center"><em>主板</em></th><th class="text-center"><em>科创板</em></th><th class="text-center"><em>主板</em></th><th class="text-center"><em>科创板</em></th></tr></thead>');
                tablist.push('<tbody>');
                tablist.push('<tr><td style="vertical-align: middle;" rowspan="9"><p style="width:20px;margin: 0 auto;line-height: 16px;font-weight: bold;">普通股</p></td><td style="vertical-align: middle;" rowspan="3"><em>首发</em></td><td><em>公开发行</em></td>\
    <td class="text-center"><em>' + renum(jsondata.zb1mMBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb1mIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb1yMBComCnt) + '</em></td><td class="text-center"><em>' +
                    renum(jsondata.kcb1yIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb1mMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb1mIBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb1yMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb1yIBIssAmt, 2) + '</em></td></tr>');
                tablist.push('<tr><td><em>超额配售</em></td>\
    <td class="text-center"><em>' + renum(jsondata.zb2mMBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb2mIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb2yMBComCnt) + '</em></td><td class="text-center"><em>' +
                    renum(jsondata.kcb2yIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb2mMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb2mIBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb2yMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb2yIBIssAmt, 2) + '</em></td></tr>');
                tablist.push('<tr class="level_2"><td><em>首发小计</em></td>\
    <td colspan="2" class="text-center"><em>' + renum(jsondata.scfxxjmComCnt) + '</em></td><td colspan="2" class="text-center"><em>' + renum(jsondata.scfxxjyComCnt) + '</em></td><td colspan="2" class="text-center"><em>' + renum(jsondata.scfxxjmIssAmt, 2) + '</em></td><td colspan="2" class="text-center"><em>' + renum(jsondata.scfxxjyIssAmt, 2) + '</em></td></tr>');
                tablist.push('<tr><td style="vertical-align: middle;" rowspan="5"><em>再发</em></td><td><em>公开增发</em></td>\
    <td class="text-center"><em>' + renum(jsondata.zb4mMBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb4mIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb4yMBComCnt) + '</em></td><td class="text-center"><em>' +
                    renum(jsondata.kcb4yIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb4mMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb4mIBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb4yMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb4yIBIssAmt, 2) + '</em></td></tr>');
                tablist.push('<tr><td><em>定向增发</em></td>\
    <td class="text-center"><em>' + renum(jsondata.zb5mMBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb5mIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb5yMBComCnt) + '</em></td><td class="text-center"><em>' +
                    renum(jsondata.kcb5yIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb5mMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb5mIBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb5yMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb5yIBIssAmt, 2) + '</em></td></tr>');
                tablist.push('<tr><td><em>配股</em></td>\
    <td class="text-center"><em>' + renum(jsondata.zb6mMBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb6mIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb6yMBComCnt) + '</em></td><td class="text-center"><em>' +
                    renum(jsondata.kcb6yIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb6mMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb6mIBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb6yMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb6yIBIssAmt, 2) + '</em></td></tr>');
                tablist.push('<tr><td><em>转债转股</em></td>\
    <td class="text-center"><em>' + renum(jsondata.zb7mMBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb7mIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb7yMBComCnt) + '</em></td><td class="text-center"><em>' +
                    renum(jsondata.kcb7yIBComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb7mMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb7mIBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.zb7yMBIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.kcb7yIBIssAmt, 2) + '</em></td></tr>');
                tablist.push('<tr class="level_2"><td><em>再发小计</em></td>\
    <td colspan="2" class="text-center"><em>' + renum(jsondata.zcfxxjmComCnt) + '</em></td><td colspan="2" class="text-center"><em>' + renum(jsondata.zcfxxjyComCnt) + '</em></td><td colspan="2" class="text-center"><em>' + renum(jsondata.zcfxxjmIssAmt, 2) + '</em></td><td colspan="2" class="text-center"><em>' + renum(jsondata.zcfxxjyIssAmt, 2) + '</em></td></tr>');
                tablist.push('<tr class="level_1"><td colspan="2"><em>普通股合计</em></td>\
    <td colspan="2" class="text-center"><em>' + renum(jsondata.gphjmComCnt) + '</em></td><td colspan="2" class="text-center"><em>' + renum(jsondata.gphjyComCnt) + '</em></td><td colspan="2" class="text-center"><em>' + renum(jsondata.gphjmIssAmt, 2) + '</em></td><td colspan="2" class="text-center"><em>' + renum(jsondata.gphjyIssAmt, 2) + '</em></td></tr>');
                tablist.push('<tr><td style="vertical-align: middle;" rowspan="3"><p style="width:20px;margin: 0 auto;line-height: 16px;font-weight: bold;">优先股</p></td><td colspan="2"><em>首次发行</em></td>\
    <td class="text-center"><em>' + renum(jsondata.zbyx1mComCnt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyx1mComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zbyx1yComCnt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyx1yComCnt) +
                    '</em></td><td class="text-center"><em>' + renum(jsondata.zbyx1mIssAmt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyx1mIssAmt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zbyx1yIssAmt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyx1yIssAmt) + '</em></td></tr>');
                tablist.push('<tr><td colspan="2"><em>再次发行</em></td>\
    <td class="text-center"><em>' + renum(jsondata.zbyx2mComCnt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyx2mComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zbyx2yComCnt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyx2yComCnt) +
                    '</em></td><td class="text-center"><em>' + renum(jsondata.zbyx2mIssAmt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyx2mIssAmt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zbyx2yIssAmt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyx2yIssAmt) + '</em></td></tr>');
                tablist.push('<tr class="level_1"><td colspan="2"><em>优先股合计</em></td>\
    <td class="text-center"><em>' + renum(jsondata.zbyxhjmComCnt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyxhjmComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zbyxhjyComCnt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyxhjyComCnt) +
                    '</em></td><td class="text-center"><em>' + renum(jsondata.zbyxhjmIssAmt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyxhjmIssAmt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zbyxhjyIssAmt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbyxhjyIssAmt) + '</em></td></tr>');
                tablist.push('<tr class="level_1"><td colspan="3"><em>公司债融资</em></td>\
    <td class="text-center"><em>' + renum(jsondata.zbgszhjmComCnt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbgszhjmComCnt) + '</em></td><td class="text-center"><em>' + renum(jsondata.zbgszhjyComCnt) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbgszhjyComCnt) +
                    '</em></td><td class="text-center"><em>' + renum(jsondata.zbgszhjmIssAmt, 2) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbgszhjmIssAmt, 2) + '</em></td><td class="text-center"><em>' + renum(jsondata.zbgszhjyIssAmt, 2) + '</em></td><td class="text-center"><em>' + renumKcb(jsondata.kcbgszhjyIssAmt, 2) + '</em></td></tr>');
                tablist.push('</tbody>');
                $('.table').html(tablist.join(""));
                $('.sse_table_T03 .table thead tr th b,.sse_table_T03 .table tbody tr.level_2 td em,.sse_table_T03 .table tbody tr.level_1 td em').css('padding-left', '0');
                $('.sse_table_T03 .table thead tr th em').css('padding-right', '0');
                $('.table.b1sd>tbody>tr>td').css('text-align', 'center');
                datatime = callBackData.searchDate;
                datatime = datatime.substring(0, 7) + '-99';
                datatime = shijian(datatime, 0, 0, 0);
                $('.sse_table_title2').find('p').html('* 截止时间：' + datatime.substring(0, 4) + '年' + datatime.substring(5, 7) + '月' + datatime.substring(8, 10) + '日');
            }
        });
    });
}
//===============================股本总貌=========================================

var $jssearch_gbzm = $(".search_gbzm_kcb");
if ($jssearch_gbzm.length) {
    var tabarr = [];
    var tabnew = [];
    var tabheadarr = ['有限售流通股', '其中：特别表决权股', '无限售流通股合计', '无限售流通A股/CDR', '境内上市外资股（B股）', '境内上市股票合计'];
    var cnt = 0;
    for (var j = 0; j < tabheadarr.length; j++) {
        for (var i = 0; i < tablezm.data.length; i++) {
            if (tablezm.data[i] != '') {
                if (tablezm.data[i][0] == tabheadarr[j]) {
                    tabnew[cnt] = tablezm.data[i];
                    cnt++;
                }
            }

        }
    }
    if (tabnew.length > 0) {
        tabarr.push('<thead><tr><th class="td_text_center"><em>股份类型</em></th><th class="td_text_center"><em>股本数<br/>(亿股/亿份)</em></th><th class="td_text_center"><em>比例<br/>（%）</em></th><th class="td_text_center"><em>较上月增减<br/>(亿股/亿份)</em></th></tr></thead>');
        tabarr.push('<tbody>');
        tabarr.push('<tr class="level_1"><td ><em>有限售流通股</em></td><td><i>' + ifundefindTurn(tabnew[0][1]) + '</i></td><td><i>' + ifundefindTurn(tabnew[0][2]) + '</i></td><td><i>' + ifundefindTurn(tabnew[0][3]) + '</i></td></tr>');
        tabarr.push('<tr class="level_2"><td class="text-indent:1em;"><em>其中：特别表决权股</em></td><td><i>' + ifundefindTurn(tabnew[1][1]) + '</i></td><td><i>' + ifundefindTurn(tabnew[1][2]) + '</i></td><td><i>' + ifundefindTurn1(tabnew[1][3]) + '</i></td></tr>');
        tabarr.push('<tr class="level_1"><td class=""><em>无限售流通股</em></td><td><i>' + ifundefindTurn(tabnew[2][1]) + '</i></td><td><i>' + ifundefindTurn(tabnew[2][2]) + '</i></td><td><i>' + ifundefindTurn(tabnew[2][3]) + '</i></td></tr>');
        tabarr.push('<tr class="level_2"><td class="text-indent:1em;"><em>无限售流通A股/CDR</em></td><td><i>' + ifundefindTurn(tabnew[3][1]) + '</i></td><td><i>' + ifundefindTurn(tabnew[3][2]) + '</i></td><td><i>' + ifundefindTurn(tabnew[3][3]) + '</i></td></tr>');
        tabarr.push('<tr class="level_2"><td class="text-indent:1em;"><em>境内上市外资股（B股）</em></td><td><i>' + ifundefindTurn(tabnew[4][1]) + '</i></td><td><i>' + ifundefindTurn(tabnew[4][2]) + '</i></td><td><i>' + ifundefindTurn1(tabnew[4][3]) + '</i></td></tr>');
        tabarr.push('</tbody><tfoot>');
        tabarr.push('<tr><td class=""><em>境内上市股份合计</em></td><td><i>' + ifundefindTurn(tabnew[5][1]) + '</i></td><td><i>' + ifundefindTurn(tabnew[5][2]) + '</i></td><td><i>' + ifundefindTurn(tabnew[5][3]) + '</i></td></tr>');
        tabarr.push('</tfoot>');
    } else {
        tabarr.push('<thead><tr><th class="td_text_center"><em>股份类型</em></th><th class="td_text_center"><em>股本数<br/>(亿股/亿份)</em></th><th class="td_text_center"><em>比例<br/>（%）</em></th><th class="td_text_center"><em>较上月增减<br/>(亿股/亿份)</em></th></tr></thead>');
        tabarr.push('<tbody>');
        tabarr.push('<tr class="level_1"><td ><em>有限售流通股</em></td><td><i>-</i></td><td><i>-</i></td><td><i>-</i></td></tr>');
        tabarr.push('<tr class="level_2"><td class="text-indent:1em;"><em>其中：特别表决权股</em></td><td><i>-</i></td><td><i>-</i></td><td><i>-</i></td></tr>');
        tabarr.push('<tr class="level_1"><td class=""><em>无限售流通股</em></td><td><i>-</i></td><td><i>-</i></td><td><i>-</i></td></tr>');
        tabarr.push('<tr class="level_2"><td class="text-indent:1em;"><em>无限售流通A股/CDR</em></td><td><i>-</i></td><td><i>-</i></td><td><i>-</i></td></tr>');
        tabarr.push('<tr class="level_2"><td class="text-indent:1em;"><em>境内上市外资股（B股）</em></td><td><i>-</i></td><td><i>-</i></td><td><i>-</i></td></tr>');
        tabarr.push('</tbody><tfoot>');
        tabarr.push('<tr><td class=""><em>境内上市股份合计</em></td><td><i>-</i></td><td><i>-</i></td><td><i>-</i></td></tr>');
        tabarr.push('</tfoot>');
    }



    $(".sse_table_T03").find("table").eq(0).show().html(tabarr.join(""));
    $('.sse_table_title2').eq(0).find('p').show().html('*截止日期：' + dateshow);
    $('.sse_table_title2').eq(1).find('p').show().html('*截止日期：' + get_lastTradeDate_global());
};

var $jssearchdataAll = $(".search_dataShowAll");
if ($jssearchdataAll.length > 0) {
    var $searchStockCode = $(".search_stockCode");
    var $qbuttonStockCode = $searchStockCode.find("#btnQuery");
    $qbuttonStockCode.on("click", function() {
        var stockCode = $('#inputCode').val();
        if (stockCode == "" || stockCode == "证券代码或简称") {
            alert("请输入证券代码或简称");
        } else if (isNaN(stockCode)) {
            alert("证券代码必须为6位数字，请根据智能提示选择证券代码后查询");
        } else if (stockCode.length != 6) {
            alert("证券代码必须为6位数字");
        } else {
            location.href = '/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=' + stockCode;
        }
    });
    var $qbuttonDataAll = $jssearchdataAll.find("#btnQuery");
    var lastTradeDate = get_lastTradeDate_global();
    $jssearchdataAll.find('#start_date').val(get_systemDate_global().substring(0, 7) + "-01");
    $jssearchdataAll.find('#end_date').val(get_systemDate_global());
    //$('.data-time').find('span').html('截止日：'+lastTradeDate.substring(0,4)+'年'+lastTradeDate.substring(5,7)+'月'+lastTradeDate.substring(8,10)+'日');
    var tabLine = tableDataLine;
    //$('.sse_img_infor_rank').find('li').eq(0).find('b').html(tabLine.param1);
    //$('.sse_img_infor_rank').find('li').eq(1).find('b').html(tabLine.param2);
    //$('.sse_img_infor_rank').find('li').eq(2).find('b').html(tabLine.param3);
    var tablist = [];
    tablist.push('<thead><tr><th></th><th><b>股票</b></th><th><b>主板<b></th><th><b>科创板</b></th></tr></thead>');
    tablist.push('<tbody>');
    tablist.push('<tr class="level_3" ><td><em>公司数</em></td><td><i>' + ifundefindTurn(tabLine.line_1[1]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_1[2]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_1[3]) + '</i></td></tr>');
    tablist.push('<tr class="level_3" ><td><em>总股本</em></td><td><i>' + ifundefindTurn(tabLine.line_2[1]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_2[2]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_2[3]) + '</i></td></tr>');
    tablist.push('<tr class="level_3" ><td><em>无限售条件流通股</em></td><td><i>' + ifundefindTurn(tabLine.line_3[1]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_3[2]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_3[3]) + '</i></td></tr>');
    tablist.push('<tr class="level_3" ><td><em>筹资(金额/公司数)</em></td><td><i>' + ifundefindTurn(tabLine.line_4[1]) + '/' + ifundefindTurn(tabLine.line_4[2]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_4[3]) + '/' + ifundefindTurn(tabLine.line_4[4]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_4[5]) + '/' + ifundefindTurn(tabLine.line_4[6]) + '</i></td></tr>');
    tablist.push('<tr class="level_3" ><td><em>首发(金额/公司数)</em></td><td><i>' + ifundefindTurn(tabLine.line_5[1]) + '/' + ifundefindTurn(tabLine.line_5[2]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_5[3]) + '/' + ifundefindTurn(tabLine.line_5[4]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_5[5]) + '/' + ifundefindTurn(tabLine.line_5[6]) + '</i></td></tr>');
    tablist.push('<tr class="level_3" ><td><em>增发(金额/公司数)</em></td><td><i>' + ifundefindTurn(tabLine.line_6[1]) + '/' + ifundefindTurn(tabLine.line_6[2]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_6[3]) + '/' + ifundefindTurn(tabLine.line_6[4]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_6[5]) + '/' + ifundefindTurn(tabLine.line_6[6]) + '</i></td></tr>');
    tablist.push('<tr class="level_3" ><td><em>分红(金额/公司数)</em></td><td><i>' + ifundefindTurn(tabLine.line_7[1]) + '/' + ifundefindTurn(tabLine.line_7[2]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_7[3]) + '/' + ifundefindTurn(tabLine.line_7[4]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_7[5]) + '/' + ifundefindTurn(tabLine.line_7[6]) + '</i></td></tr>');
    tablist.push('<tr class="level_3" ><td><em>送股公司数</em></td><td><i>' + ifundefindTurn(tabLine.line_8[1]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_8[2]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_8[3]) + '</i></td></tr>');
    tablist.push('<tr class="level_3" ><td><em>配股公司数</em></td><td><i>' + ifundefindTurn(tabLine.line_9[1]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_9[2]) + '</i></td><td><i>' + ifundefindTurn(tabLine.line_9[3]) + '</i></td></tr>');

    tablist.push('</tbody>');
    $('.sse_table_T03').find('.sse_table_conment').find('p').html("注：<br/> 1.股本数单位为亿股，金额为亿元<br />2.筹资按上市日统计，分红按除权日统计");
    $('.table').html(tablist.join(""));

    /*
     *  数据展示
     */
    function doShowDataAll(callBackData) {
        var tablistShow = [];
        if (callBackData.result != undefined && callBackData.result != "") {
            var dataUsed = callBackData.result;
            tablistShow.push('<thead><tr><th></th><th><b>股票</b></th><th><b>主板<b></th><th><b>科创板</b></th></tr></thead>');
            tablistShow.push('<tbody>');
            tablistShow.push('<tr class="level_3" ><td><em>公司数</em></td><td><i>' + ifundefindTurn(dataUsed.companyNum) + '</i></td><td><i>' + ifundefindTurn(dataUsed.zbCompanyNum) + '</i></td><td><i>' + ifundefindTurn(dataUsed.kcbCompanyNum) + '</i></td></tr>');
            tablistShow.push('<tr class="level_3" ><td><em>总股本</em></td><td><i>' + ifundefindTurn(dataUsed.totalShares) + '</i></td><td><i>' + ifundefindTurn(dataUsed.totalZBShares) + '</i></td><td><i>' + ifundefindTurn(dataUsed.totalKCBShare) + '</i></td></tr>');
            tablistShow.push('<tr class="level_3" ><td><em>无限售条件流通股</em></td><td><i>' + ifundefindTurn(dataUsed.totalUnlimitedShares) + '</i></td><td><i>' + ifundefindTurn(dataUsed.unlimitedAShares) + '</i></td><td><i>' + ifundefindTurn(dataUsed.unlimitedKCBShares) + '</i></td></tr>');
            tablistShow.push('<tr class="level_3" ><td><em>筹资(金额/公司数)</em></td><td><i>' + ifundefindTurn(dataUsed.totalFundsRaisedAmt) + '/' + ifundefindTurn(dataUsed.totalFundsRaised) + '</i></td><td><i>' + ifundefindTurn(dataUsed.fundsRaisedAmtZB) + '/' + ifundefindTurn(dataUsed.fundsRaisedZB) + '</i></td><td><i>' + ifundefindTurn(dataUsed.fundsRaisedAmtKCB) + '/' + ifundefindTurn(dataUsed.fundsRaisedKCB) + '</i></td></tr>');
            tablistShow.push('<tr class="level_3" ><td><em>首发(金额/公司数)</em></td><td><i>' + ifundefindTurn(dataUsed.issueSAmt) + '/' + ifundefindTurn(dataUsed.issueS) + '</i></td><td><i>' + ifundefindTurn(dataUsed.issueSAmtZB) + '/' + ifundefindTurn(dataUsed.issueSZB) + '</i></td><td><i>' + ifundefindTurn(dataUsed.issueSAmtKCB) + '/' + ifundefindTurn(dataUsed.issueSKCB) + '</i></td></tr>');
            tablistShow.push('<tr class="level_3" ><td><em>增发(金额/公司数)</em></td><td><i>' + ifundefindTurn(dataUsed.issueZAmt) + '/' + ifundefindTurn(dataUsed.issueZ) + '</i></td><td><i>' + ifundefindTurn(dataUsed.issueZAmtZB) + '/' + ifundefindTurn(dataUsed.issueZZB) + '</i></td><td><i>' + ifundefindTurn(dataUsed.issueZAmtKCB) + '/' + ifundefindTurn(dataUsed.issueZKCB) + '</i></td></tr>');
            tablistShow.push('<tr class="level_3" ><td><em>分红(金额/公司数)</em></td><td><i>' + ifundefindTurn(dataUsed.totalDividendAmt) + '/' + ifundefindTurn(dataUsed.totalDividendCash) + '</i></td><td><i>' + ifundefindTurn(dataUsed.dividendAmtZB) + '/' + ifundefindTurn(dataUsed.dividendcashZB) + '</i></td><td><i>' + ifundefindTurn(dataUsed.dividendAmtKCB) + '/' + ifundefindTurn(dataUsed.dividendcashKCB) + '</i></td></tr>');
            tablistShow.push('<tr class="level_3" ><td><em>送股公司数</em></td><td><i>' + ifundefindTurn(dataUsed.totalDividentShare) + '</i></td><td><i>' + ifundefindTurn(dataUsed.dividentShareZB) + '</i></td><td><i>' + ifundefindTurn(dataUsed.dividentShareKCB) + '</i></td></tr>');
            tablistShow.push('<tr class="level_3" ><td><em>配股公司数</em></td><td><i>' + ifundefindTurn(dataUsed.totalRight) + '</i></td><td><i>' + ifundefindTurn(dataUsed.rightZB) + '</i></td><td><i>' + ifundefindTurn(dataUsed.rightKCB) + '</i></td></tr>');
            tablistShow.push('</tbody>');
            $('.sse_table_T03').find('.sse_table_conment').find('p').html("注：<br/> 1.股本数单位为亿股，金额为亿元<br />2.筹资按上市日统计，分红按除权日统计");
            $('.table').html(tablistShow.join(""));
        } else {
            tablist.push('<thead><tr><th></th><th><b>股票</b></th><th><b>主板<b></th><th><b>科创板</b></th></tr></thead>');
            tablist.push('<tbody>');
            tablist.push('<tr class="level_3" ><td><em>公司数</em></td><td><i>-</i></td><td><i>-</i></td><td><i>-</i></td></tr>');
            tablist.push('<tr class="level_3" ><td><em>总股本</em></td><td><i>-</i></td><td><i>-</i></td><td><i>-</i></td></tr>');
            tablist.push('<tr class="level_3" ><td><em>无限售条件流通股</em></td><td><i>-</i></td><td><i>-</i></td><td><i>-</i></td></tr>');
            tablist.push('<tr class="level_3" ><td><em>筹资(金额/公司数)</em></td><td><i>-/-</i></td><td><i>-/-</i></td><td><i>-/-</i></td></tr>');
            tablist.push('<tr class="level_3" ><td><em>首发(金额/公司数)</em></td><td><i>-/-</i></td><td><i>-/-</i></td><td><i>-/-</i></td></tr>');
            tablist.push('<tr class="level_3" ><td><em>增发(金额/公司数)</em></td><td><i>-/-</i></td><td><i>-/-</i></td><td><i>-/-</i></td></tr>');
            tablist.push('<tr class="level_3" ><td><em>分红(金额/公司数)</em></td><td><i>-/-</i></td><td><i>-/-</i></td><td><i>-/-</i></td></tr>');
            tablist.push('<tr class="level_3" ><td><em>送股公司数</em></td><td><i>-</i></td><td><i>-</i></td><td><i>-</i></td></tr>');
            tablist.push('<tr class="level_3" ><td><em>配股公司数</em></td><td><i>-</i></td><td><i>-</i></td><td><i>-</i></td></tr>');
            $('.sse_table_T03').find('.sse_table_conment').find('p').html("注：<br/> 1.股本数单位为亿股，金额为亿元<br />2.筹资按上市日统计，分红按除权日统计");
            $('.table').html(tablistShow.join(""));
        }

    }

    /*
     *ajax请求
     */
    function ajaxDataAll(jsonObj) {
        showloading();
        $.ajax({
            url: jsonObj.url,
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: jsonObj.param,

            success: function(callBackData) {
                doShowDataAll(callBackData);
                hideloading();
            }
        });
    }

    $qbuttonDataAll.on("click", function() {
        var dateStart = $jssearchdataAll.find("#start_date").val();
        var dateEnd = $jssearchdataAll.find("#end_date").val();
        if (dateEnd < dateStart) {
            alert("结束日期应大于开始日期！");
        } else if (dateEnd > get_systemDate_global()) {
            alert("您选择的日期超出检索范围，请重输。");

        } else if (dateStart < "1990-12-19") {
            alert("您选择的日期超出检索范围，请重输。");

        } else {
            var action = "queryNewDataStatistics";
            ajaxDataAll({
                url: sseQueryURL + 'security/stock/' + action + '.do',
                pageData: '#js_data', //数据输出ID选择器
                param: {
                    "startDate": dateStart,
                    "endDate": dateEnd
                }
            });
        }
    });
}

//市值排名
//科创板股票市价总值排名前十名
var sysTime = '2022-01-07';
var $searchSzzj = $(".search_szzj");
if ($searchSzzj.length > 0) {
    $searchSzzj.parent().find('.js_tableT01').find(".sse_table_title2").show().find("p").text("数据更新日期：" + sysTime);
    var straa1 = '<div></div>';
    $searchSzzj.parent().find(".table").after(straa1); //统计
    showajaxSzzj(sysTime);
    // 添加跳转链接以及备注
    var htm = "<a href='/market/stockdata/marketvalue/star/' target='_blank' style='font-size:12px;font-weight:normal;float:inherit' title='数据截止到2022年1月9日'>(此栏目为历史数据，更多数据点击此处)</a>"
    $(".sse_title_common").eq(0).find("h2").html("科创板股票市价总值排名前十名" + htm);
    $(".sse_title_common").eq(1).find("h2").html("科创板股票流通市值排名前十名 " + htm);
    //$(".sse_table_title2").eq(0).show().find("p").html("数据更新日期："+ownDateQ);
    var nowTime = $searchSzzj.find("#start_date2").val();
    //$searchSzzj.find("#start_date2").val(sysTime);
    //修改日期显示一致问题
    var showdate = '';
    if ($(".sse_table_title2").eq(0).find("p").html() != '<tr><td colspan="50">未找到，只有0条数据！</td></tr>') {
        showdate = $(".sse_table_title2").eq(0).find("p").html().substring(7, 17);
        $searchSzzj.find("#start_date2").val(showdate);
    }

    var $searchSzzj_btn = $searchSzzj.parent().find("#btnQuery");
    $searchSzzj_btn.click(function() {
        var searchSzzj_time = $searchSzzj.find("#start_date2").val();
        //$searchSzzj.parent().find('.js_tableT01').find(".sse_table_title2").show().find("p").text("数据更新日期："+searchSzzj_time);

        var nowTime = $searchSzzj.find("#start_date2").val();
        var turnDateStart = new Date(Date.parse(nowTime)); //转换成Data();
        var theDayChoice = turnDateStart.getDay();
        if (nowTime > get_systemDate_global()) {
            alert("您选择的日期超出检索范围，请重输。")
        } else if (nowTime > get_lastTradeDate_global() && (!get_whetherTradeDate_global())) {
            alert("查询时间不是交易日。");
        } else if (theDayChoice == 0 || theDayChoice == 6) {
            alert("查询时间不是交易日。");
        } else if (nowTime < "1990-12-19") {
            alert("您选择的日期超出检索范围，请重输。")
        } else if (nowTime == get_systemDate_global() && (!get_whetherTradeDate_global())) {
            alert("查询时间不是交易日。")
        } else {
            showajaxSzzj(searchSzzj_time);
        }

    });
    //ajax
    function showajaxSzzj(searchSzzj_time) {
        showloading();
        var searchSzzjTime = searchSzzj_time.replace(/-/g, '')
            // var action = "queryKCBTopMktValByPage";
        var sqlId = 'MKT_VALUE_TS10'
        var tempData = {
            // url: sseQueryURL + 'marketdata/tradedata/' + action + '.do?',
            url: sseQueryURL + 'commonSoaQuery.do?',
            params: {
                // "isPagination": true, //是否分页
                'sqlId': sqlId,
                "tradeDate": searchSzzjTime,
                'boardType': 1,
            }
        };

        jQuery.ajax({
            url: tempData.url,
            type: "post",
            dataType: "jsonp",
            jsonp: 'jsonCallBack',
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            async: false,
            cache: false,
            data: tempData.params,
            success: function(dataJson) {
                var htmlArrIsNull = [];
                htmlArrIsNull.push('<tr><th>名次</th><th>股票代码</th><th>股票简称</th><th>市价总值(万元)</th><th>所占总市值的比例(%)</th></tr>');
                htmlArrIsNull.push('<tr><td colspan="50">对不起! 共找到0条记录</td></tr>');
                $(".sse_table_T01").eq(0).find('.table').html(htmlArrIsNull.join(""));
                ajaxSearchSzzj(dataJson);

            },
            complete: function() {
                hideloading();
            },
            error: function(e) {}
        });
    }

    //数据填充
    var ajaxSearchSzzj = function(dataJson) {
        var htmlArr2 = [];

        if (dataJson.result.length > 0) {
            htmlArr2.push('<tr><th>名次</th><th>股票代码</th><th>股票简称</th><th>市价总值(万元)</th><th>所占总市值的比例(%)</th></tr>');
            for (var i = 0; i < dataJson.result.length; i++) {
                var data = dataJson.result[i];

                htmlArr2.push('<tr>');
                htmlArr2.push('<td>' + data.rank + '</td>');
                htmlArr2.push('<td><a href="/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=' + data.product + '&FULLNAME=' + data.productName + '" target="_blank">' + data.product + '</td>');
                htmlArr2.push('<td>' + data.productName + '</td>');
                htmlArr2.push('<td><div class="align_right">' + data.marketValue + '</div></td>');
                htmlArr2.push('<td><div class="align_right">' + data.marketValuePer + '</div></td>');
                htmlArr2.push('</tr>');
            }
            $searchSzzj.parent().find('.js_tableT01').find(".sse_table_title2").show().find("p").text("数据更新日期：" + dataJson.result[0].txDate);
            $searchSzzj.parent().find(".table").siblings('div').html(dataJson.result[0].totalPer != '' ? ("所占总市值的比例总计：" + dataJson.result[0].totalPer + "%") : '');
        } else {
            $searchSzzj.parent().find('.js_tableT01').find(".sse_table_title2").show().find("p").text("");
            $searchSzzj.parent().find(".table").siblings('div').html("");
            htmlArr2.push('<tr><td  colspan="50">对不起! 共找到0条记录</td></tr>');
        }
        $(".sse_table_T01").eq(0).find('.table').html(htmlArr2.join(""));

    }

}

//科创板股票流通市值排名前十名
var sysTime = '2022-01-07';
var $searchLtsz = $(".search_ltsz");
if ($searchLtsz.length > 0) {
    var straa1 = '<div></div>';
    $searchLtsz.parent().find(".table").after(straa1); //统计
    $searchLtsz.parent().find('.js_tableT01').find(".sse_table_title2").show().find("p").text("数据更新日期：" + sysTime);
    showajaxLtsz(sysTime);
    //$(".sse_table_title2")eq(1).show().find("p").html("数据更新日期："+ownDateW);
    //$searchLtsz.find("#start_date2").val(sysTime);
    //修改日期显示一致问题
    var showdate = '';
    if ($(".sse_table_title2").eq(0).find("p").html() != '<tr><td colspan="50">未找到，只有0条数据！</td></tr>') {
        showdate = $(".sse_table_title2").eq(0).find("p").html().substring(7, 17);
        $searchLtsz.find("#start_date2").val(showdate);
    }
    var $searchLtsz_btn = $searchLtsz.parent().find("#btnQuery");
    $searchLtsz_btn.click(function() {
        var searchLtsz_time = $searchLtsz.find("#start_date2").val();

        //$searchLtsz.parent().find('.js_tableT01').find(".sse_table_title2").show().find("p").text("数据更新日期："+searchLtsz_time);
        // showajaxLtsz(searchLtsz_time);
        var nowTime = $searchLtsz.find("#start_date2").val();
        var turnDateStart = new Date(Date.parse(nowTime)); //转换成Data();
        var theDayChoice = turnDateStart.getDay();

        if (nowTime > get_systemDate_global()) {
            alert("您选择的日期超出检索范围，请重输。")
        } else if (nowTime > get_lastTradeDate_global() && (!get_whetherTradeDate_global())) {
            alert("查询时间不是交易日。");
        } else if (theDayChoice == 0 || theDayChoice == 6) {
            alert("查询时间不是交易日。");
        } else if (nowTime < "1990-12-19") {
            alert("您选择的日期超出检索范围，请重输。")
        } else if (nowTime == get_systemDate_global() && (!get_whetherTradeDate_global())) {
            alert("查询时间不是交易日。")
        } else {
            showajaxLtsz(searchLtsz_time);
        }
    });
    //ajax
    function showajaxLtsz(searchLtsz_time) {
        showloading();
        var searchLtszTime = searchLtsz_time.replace(/-/g, '')
        var tempData = {
            url: sseQueryURL + 'commonSoaQuery.do',
            params: {
                // "isPagination": true, //是否分页
                'sqlId': 'NEGO_VALUE_TS10',
                'boardType': 1,
                "tradeDate": searchLtszTime,
            }
        };

        jQuery.ajax({
            url: tempData.url,
            type: "post",
            dataType: "jsonp",
            jsonp: 'jsonCallBack',
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            async: false,
            cache: false,
            data: tempData.params,
            success: function(dataJson) {
                var htmlArrIsNull = [];
                htmlArrIsNull.push('<tr><th>名次</th><th>股票代码</th><th>股票简称</th><th>流通市值(万元)</th><th>所占流通市值的比例(%)</th></tr>');
                htmlArrIsNull.push('<tr><td colspan="50">对不起! 共找到0条记录</td></tr>');
                $(".sse_table_T01").eq(1).find('.table').html(htmlArrIsNull.join(""));
                ajaxSearchLtsz(dataJson);

            },
            complete: function() {
                hideloading();
            },
            error: function(e) {}
        });
    }

    //数据填充
    var ajaxSearchLtsz = function(dataJson) {
        var htmlArr2 = [];
        if (dataJson.result.length > 0) {
            htmlArr2.push('<tr><th>名次</th><th>股票代码</th><th>股票简称</th><th>流通市值(万元)</th><th>所占流通市值的比例(%)</th></tr>');
            for (var i = 0; i < dataJson.result.length; i++) {
                var data = dataJson.result[i];

                htmlArr2.push('<tr>');
                htmlArr2.push('<td>' + data.rank + '</td>');
                htmlArr2.push('<td><a href="/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=' + data.product + '&FULLNAME=' + data.productName + '" target="_blank">' + data.product + '</td>');
                htmlArr2.push('<td>' + data.productName + '</td>');
                htmlArr2.push('<td><div class="align_right">' + data.nego + '</div></td>');
                htmlArr2.push('<td><div class="align_right">' + data.negoPer + '</div></td>');
                htmlArr2.push('</tr>');
            }

            $searchLtsz.parent().find('.js_tableT01').find(".sse_table_title2").show().find("p").text("数据更新日期：" + dataJson.result[0].txDate);
            $searchLtsz.parent().find(".table").siblings('div').html(dataJson.result[0].totalPer != '' ? ("所占总市值的比例总计：" + dataJson.result[0].totalPer + "%") : '');
        } else {
            $searchLtsz.parent().find('.js_tableT01').find(".sse_table_title2").show().find("p").text("");
            $searchLtsz.parent().find(".table").siblings('div').html("");
            htmlArr2.push('<tr><td  colspan="50">对不起! 共找到0条记录</td></tr>');
        }
        $searchLtsz.parent().find(".table").siblings('div')
        $(".sse_table_T01").eq(1).find('.table').html(htmlArr2.join(""));

    }

}

//个股
function GetQueryString(name) {
    var reg = new RegExp("(^|&)" + name + "=([^&]*)(&|$)");
    var code = /<(S*?)[^%3C|<]*%3C|<.*?|%3E|>.*?/
    var r = window.location.search.substr(1).match(reg);
    var params = '';

    if (r != null) {
        var h = unescape(r[2]);
        var code = /<(S*?)[^%3C|<]*%3C|<.*?|%3E|>.*?/
        if (code.test(h)) {
            params = h.replace(/%3C|<|%3E|>|%20/g, "");
        } else {
            params = h;
        }
        return mergerByAbsorption(params);
    } else {
        return null;
    }
}

function fakeLabelChoiceSpe(code) {
    var arr = [];
    arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/bonds/assets/products/assetsinfo/basic/index.shtml?CODE=' + code + '"><span>基本信息</span></a></div></div>');
    //arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/bonds/assets/products/assetsinfo/turnover/index.shtml?CODE=' + code + '"><span>成交概况</span></a></div></div>');
    $('.swiper-wrapper').html(arr.join(""));
    doActiveList();
}
/**
 * 股本结构、筹资情况、利润分配、成交概况
 * 修改tab根据股票类型（主板、可创办）分别显示对应的标题
 */
function fakeLabelChoice(code) {
    $.ajax({
        url: sseQueryURL + "commonQuery.do",
        type: "post",
        dataType: "jsonp",
        jsonp: "jsonCallBack",
        async: false,
        jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
        data: {
            'isPagination': false,
            'sqlId': 'COMMON_SSE_ZQLX_C',
            'SEC_CODE': code
        },
        success: function(dataJson) {
            var data = dataJson.result[0];
            if (data) {
                // var stockType = data.TYPE;
                var stockType = data.SUB_TYPE;
                var htmlStr = '';
                //ASH  以人民币交易的股票（主板） KSH  以人民币交易的股票（科创板）
                stockType == 'ASH' ? htmlStr = '高管人员' : htmlStr = '高管及核心技术人员';
                var arr = [];
                arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=' + code + '"><span>公司概况</span></a></div></div>');
                //arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/capital/index.shtml?COMPANY_CODE=' + code + '"><span>股本结构</span></a></div></div>');
                arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/financing/index.shtml?COMPANY_CODE=' + code + '"><span>筹资情况</span></a></div></div>');
                arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/profit/index.shtml?COMPANY_CODE=' + code + '"><span>利润分配</span></a></div></div>');
                arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/turnover/index.shtml?COMPANY_CODE=' + code + '"><span>成交概况</span></a></div></div>');
                arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/price/index.shtml?COMPANY_CODE=' + code + '"><span>行情图表</span></a></div></div>');
                arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/announcement/index.shtml?productId=' + code + '"><span>公司公告</span></a></div></div>');
                arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/summary/index.shtml?COMPANY_CODE=' + code + '"><span>公告摘要</span></a></div></div>');
                arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/rules/index.shtml?COMPANY_CODE=' + code + '"><span>公司章程</span></a></div></div>');
                arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/governance/index.shtml?COMPANY_CODE=' + code + '"><span>治理细则</span></a></div></div>');
                arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/meeting/index.shtml?COMPANY_CODE=' + code + '"><span>股东大会资料</span></a></div></div>');
                arr.push('<div class="swiper-slide"><div class="title alpha-top"><a href="/assortment/stock/list/info/executives/index.shtml?COMPANY_CODE=' + code + '"><span>' + htmlStr + '</span></a></div></div>');
                $('.swiper-wrapper').html(arr.join(""));
                doActiveList();
            }
        }
    });
}

function doActiveList() {
    var $doActive = $(".swiper-wrapper").find(".swiper-slide");
    var pageTitle = $.trim($(".page_big_title").text());
    $.each($doActive, function(i) {
        if ($doActive.eq(i).text().indexOf(pageTitle) != -1) {
            $doActive.eq(i).addClass("active");
        }
    });


    require(['sse'], function() {
        loadList();
    })
}

function ifNullTurn(turnParam) {
    if (turnParam == null || turnParam == "" || turnParam == "null" || turnParam == "NULL") {
        return "-";
    } else {
        return turnParam;
    }
}

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
//利润分配
var $StockViewProfit = $(".search_stockListProfit");
if ($StockViewProfit.length > 0) {
    var urlProductid = GetQueryString("COMPANY_CODE");
    var codeA = "";
    var codeB = "";

    var htm = "<a href='/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=" + urlProductid +
        "' style='font-size:12px;font-weight:normal;float:inherit' target ='blank' title='数据截止到2022年1月9日'>(此栏目为历史数据，更多数据点击此处)</a>"
    $(".sse_title_common").eq(0).find("h2").html("分红" + htm);

    var htm1 = "<a href='/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=" + urlProductid +
        "' style='font-size:12px;font-weight:normal;float:inherit' target ='blank' title='数据截止到2022年1月9日'>(此栏目为历史数据，更多数据点击此处)</a>"
    $(".sse_title_common").eq(1).find("h2").html("送股" + htm1);
    if (urlProductid != null && urlProductid != "" && urlProductid != undefined) {
        /*
        获取标题
         */
        $.ajax({
            url: sseQueryURL + 'commonQuery.do',
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: {
                'isPagination': false,
                'sqlId': 'COMMON_SSE_ZQPZ_GP_GPLB_C',
                'productid': urlProductid
            },
            success: function(callBackData) {
                //将修改成table展示
                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                    // var isKcb;
                    var showAshare = '';
                    var showBshare = '';
                    var showKCB = '';
                    var showGF = '';
                    var ashareId_fh = '';
                    var ashareId_sg = '';
                    if (callBackData.result[0].TYPE != '0') {
                        // isKcb = true;
                        // showAshare = '科创板A股 ';
                        showBshare = '科创板CDR ';
                        if (callBackData.result[0].TYPE == '1') {
                            showAshare = '科创板A股 ';
                        } else if (callBackData.result[0].TYPE == '2') {
                            showAshare = '科创板CDR ';
                        }
                        showKCB = '万股/万份';
                        showGF = '股/份';
                        ashareId_fh = 'COMMON_SSE_ZQPZ_GG_LYFP_KCBFH_L';
                        ashareId_sg = 'COMMON_SSE_ZQPZ_GG_LYFP_KCBSG_L';

                    } else {
                        // isKcb = false;
                        showAshare = 'A股 ';
                        showBshare = 'B股 ';
                        showKCB = '万股'
                        showGF = '股';
                        ashareId_fh = 'COMMON_SSE_ZQPZ_GG_LYFP_AGFH_L';
                        ashareId_sg = 'COMMON_SSE_ZQPZ_GG_LYFP_AGSG_L';
                    }
                    codeA = callBackData.result[0].SECURITY_CODE_A;
                    codeB = callBackData.result[0].SECURITY_CODE_B;
                    if (codeA != "" && codeA != "-") {
                        //进行A股的分红送股查询
                        //【A分红】
                        $.ajax({
                            url: sseQueryURL + 'commonQuery.do',
                            type: "post",
                            dataType: "jsonp",
                            jsonp: "jsonCallBack",
                            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                            data: {
                                'isPagination': false,
                                'sqlId': ashareId_fh,
                                'productid': codeA
                            },
                            success: function(callBackData) {
                                //将修改成table展示
                                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                                    var arr = [];
                                    arr.push('<tr><th>股权登记日</th><th>股权登记日总股本(' + showKCB + ')</th><th>除息交易日</th><th>除息前日收盘价</th><th>除息报价</th><th>每' + showGF + '红利</th></tr>');
                                    // arr.push('<tr><th>含税</th><th>除税 </th></tr>');
                                    if (callBackData.result.length < 1) {
                                        $(".sse_subtitle_1").eq(0).hide();
                                        $(".tabbable").eq(0).hide();
                                    } else {
                                        for (var i = 0; i < callBackData.result.length; ++i) {
                                            arr.push('<tr><td>' + callBackData.result[i].RECORD_DATE_A + '</td><td><div class="align_right">' + stringFormatter(callBackData.result[i].ISS_VOL) + '</div></td><td>' + callBackData.result[i].EX_DIVIDEND_DATE_A + '</td>');
                                            arr.push('<td><div class="align_right">' + callBackData.result[i].LAST_CLOSE_PRICE_A + '</div></td><td><div class="align_right">' + callBackData.result[i].OPEN_PRICE_A + '</div></td><td><div class="align_right">' + callBackData.result[i].DIVIDEND_PER_SHARE2_A + '</div></td>');
                                            arr.push('</tr>');
                                        }
                                    }
                                    $(".sse_table_title2").eq(0).show();
                                    $(".sse_table_title2").eq(0).find("p").html("* 单位:元人民币");
                                    $(".sse_subtitle_1").eq(0).find("h3").html(showAshare + callBackData.result[0].SECURITY_NAME_A + "(" + callBackData.result[0].SECURITY_CODE_A + ")");
                                    $(".table").eq(0).html(arr.join(""));

                                } else {
                                    $(".sse_subtitle_1").eq(0).hide();
                                    $(".tabbable").eq(0).hide();
                                }
                            }
                        });
                        //【A送股】
                        $.ajax({
                            url: sseQueryURL + 'commonQuery.do',
                            type: "post",
                            dataType: "jsonp",
                            jsonp: "jsonCallBack",
                            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                            data: {
                                'isPagination': false,
                                'sqlId': ashareId_sg,
                                'productid': codeA
                            },
                            success: function(callBackData) {
                                //将修改成table展示
                                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                                    var arr = [];
                                    arr.push('<tr><th>股权登记日</th><th>股权登记日<br/>总股本(' + showKCB + ')</th><th>除权基准日</th><th>红股上市日</th><th>公告刊登日</th><th>送股比例<br/>(10:?)</th></tr>');
                                    if (callBackData.result.length < 1) {
                                        $(".sse_subtitle_1").eq(2).hide();
                                        $(".tabbable").eq(2).hide();
                                    } else {
                                        for (var i = 0; i < callBackData.result.length; ++i) {
                                            arr.push('<tr><td>' + callBackData.result[i].RECORD_DATE_A + '</td><td><div class="align_right">' + stringFormatter(callBackData.result[i].ISS_VOL) + '</div></td><td>' + callBackData.result[i].EX_RIGHT_DATE_A + '</td>');
                                            arr.push('<td>' + callBackData.result[i].TRADE_DATE_A + '</td><td>' + callBackData.result[i].ANNOUNCE_DATE + '</td><td><div class="align_right">' + callBackData.result[i].BONUS_RATE + '</div></td></tr>');
                                        }
                                    }
                                    $(".sse_subtitle_1").eq(2).find("h3").html(showAshare + callBackData.result[0].SECURITY_NAME_A + "(" + callBackData.result[0].SECURITY_CODE_A + ")送股");
                                    $(".table").eq(2).html(arr.join(""));

                                } else {
                                    $(".sse_subtitle_1").eq(2).hide();
                                    $(".tabbable").eq(2).hide();
                                }
                            }
                        });
                    } else {
                        $(".sse_subtitle_1").eq(0).hide();
                        $(".tabbable").eq(0).hide();
                        $(".sse_subtitle_1").eq(2).hide();
                        $(".tabbable").eq(2).hide();
                    }

                    if (codeB != "" && codeB != "-") {
                        //进行B股的分红送股查询
                        //【B分红】
                        $.ajax({
                            url: sseQueryURL + 'commonQuery.do',
                            type: "post",
                            dataType: "jsonp",
                            jsonp: "jsonCallBack",
                            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                            data: {
                                'isPagination': false,
                                'sqlId': 'COMMON_SSE_ZQPZ_GG_LYFP_BGFH_L',
                                'productid': codeA
                            },
                            success: function(callBackData) {
                                //将修改成table展示
                                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                                    var arr = [];
                                    arr.push('<tr><th>最后交易日</th><th>股权登记日</th><th>股权登记日<br/>总股本(' + showKCB + ')</th><th>除息交易日</th><th>除息前日<br/>收盘价</th><th>除息报价</th><th>每' + showGF + '红利</th><th>美元<br/>汇率</th></tr>');
                                    // arr.push('<tr><th>含税</th><th>除税 </th></tr>');
                                    if (callBackData.result.length < 1) {
                                        $(".sse_subtitle_1").eq(1).hide();
                                        $(".tabbable").eq(1).hide();
                                    } else {
                                        for (var i = 0; i < callBackData.result.length; ++i) {
                                            arr.push('<tr><td>' + callBackData.result[i].LAST_TRADE_DATE_B + '</td><td>' + callBackData.result[i].RECORD_DATE_B + '</td><td><div class="align_right">' + stringFormatter(callBackData.result[i].ISS_VOL) + '</div></td>');
                                            arr.push('<td>' + callBackData.result[i].EX_DIVIDEND_DATE_B + '</td><td><div class="align_right">' + callBackData.result[i].LAST_CLOSE_PRICE_B + '</div></td><td><div class="align_right">' + stringFormatter(callBackData.result[i].OPEN_PRICE_B) + '</div></td>');
                                            arr.push('<td><div class="align_right">' + stringFormatter(callBackData.result[i].DIVIDEND_PER_SHARE1_B) + '</div></td><td><div class="align_right">' + callBackData.result[i].EXCHANGE_RATE + '</div></td></tr>');
                                        }
                                    }
                                    $(".sse_table_title2").eq(1).show();
                                    $(".sse_table_title2").eq(1).find("p").html("* 单位:元/美金");
                                    $(".sse_subtitle_1").eq(1).find("h3").html(showBshare + callBackData.result[0].SECURITY_NAME_B + "(" + callBackData.result[0].SECURITY_CODE_B + ")");
                                    $(".table").eq(1).html(arr.join(""));

                                } else {
                                    $(".sse_subtitle_1").eq(1).hide();
                                    $(".tabbable").eq(1).hide();
                                }
                            }
                        });

                        //【B送股】
                        $.ajax({
                            url: sseQueryURL + 'commonQuery.do',
                            type: "post",
                            dataType: "jsonp",
                            jsonp: "jsonCallBack",
                            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                            data: {
                                'isPagination': false,
                                'sqlId': 'COMMON_SSE_ZQPZ_GG_LYFP_BGSG_L',
                                'productid': codeA
                            },
                            success: function(callBackData) {
                                //将修改成table展示
                                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                                    var arr = [];
                                    arr.push('<tr><th>最后交易日</th><th>股权登记日</th><th>股权登记日<br/>总股本(' + showKCB + ')</th><th>除权基准日</th><th>红股上市日</th><th>公告刊登日</th><th>送股比例<br/>(10:?)</th></tr>');
                                    if (callBackData.result.length < 1) {
                                        $(".sse_subtitle_1").eq(3).hide();
                                        $(".tabbable").eq(3).hide();
                                    } else {
                                        for (var i = 0; i < callBackData.result.length; ++i) {
                                            arr.push('<tr><td>' + callBackData.result[i].LAST_TRADE_DATE_B + '</td><td>' + callBackData.result[i].RECORD_DATE_B + '</td><td><div class="align_right">' + stringFormatter(callBackData.result[i].ISS_VOL) + '</div></td>');
                                            arr.push('<td>' + callBackData.result[i].EX_RIGHT_DATE_B + '</td><td>' + callBackData.result[i].TRADE_DATE_B + '</td><td>' + callBackData.result[i].ANNOUNCE_DATE + '</td><td><div class="align_right">' + callBackData.result[i].BONUS_RATE + '</div></td></tr>');
                                        }
                                    }
                                    $(".sse_subtitle_1").eq(3).find("h3").html(showBshare + callBackData.result[0].SECURITY_NAME_B + "(" + callBackData.result[0].SECURITY_CODE_B + ")送股");
                                    $(".table").eq(3).html(arr.join(""));

                                } else {
                                    $(".sse_subtitle_1").eq(3).hide();
                                    $(".tabbable").eq(3).hide();
                                }
                            }

                        });
                    } else {
                        $(".sse_subtitle_1").eq(1).hide();
                        $(".tabbable").eq(1).hide();
                        $(".sse_subtitle_1").eq(3).hide();
                        $(".tabbable").eq(3).hide();
                    }
                    $(".sse_list_tit1").find("span").html(callBackData.result[0].FULLNAME + urlProductid);
                    fakeLabelChoice(urlProductid);
                }
            }
        });
    }
}
//===================股票概况-成交概况================================
var $StockViewOver = $(".search_stockListOver");
if ($StockViewOver.length > 0) {
    //初始0 只有A股为1 只有B股为2 都有为3
    var urlFundId = GetQueryString("FUNDID");
    var type = 0;
    var codeA = "";
    var codeB = "";
    var showAshare = '';
    var showBshare = '';
    var showKCB = '';

    var urlProductid = GetQueryString("COMPANY_CODE");
    var linkUrl = location.host.name
    var htm = "<a href='/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=" + urlProductid +
        "' style='font-size:12px;font-weight:normal;float:inherit' target ='blank' title='日,月,年统计数据分别截止到2022年1月7日,2021年11月,2020年'>(此栏目为历史数据，更多数据点击此处)</a>"
    $(".sse_title_common").eq(0).find("h2").html("成交概况" + htm);
    $StockViewOver.find("#start_date2").attr("placeholder", "查询日期");
    var $qbuttonStockViewOver = $StockViewOver.find("#btnQuery");

    function doShowTableStockOver(obj) {

        if (codeA.length == 6) {
            //成交概况表格
            showloading();
            $.ajax({
                url: sseQueryURL + "security/fund/queryNewAllQuatAbel.do",
                type: "post",
                dataType: "jsonp",
                jsonp: "jsonCallBack",
                jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                data: {
                    "FUNDID": codeA,
                    "inMonth": obj.inMonth,
                    "inYear": obj.inYear,
                    "searchDate": obj.searchDate
                },
                success: function(callBackData) {
                    var arr = [];
                    if (callBackData.result.length > 0) {
                        arr.push('<tr><th></th><th>' + obj.searchDate.substring(0, 4) + '年' + obj.searchDate.substring(5, 7) + '月' + obj.searchDate.substring(8, 10) + '日</th><th>' + obj.inMonth.substring(0, 4) + '年' + obj.inMonth.substring(4, 6) + '月</th><th>' + obj.inYear + '年</th></tr>');
                        arr.push('<tr><td>市价总值（万元）</td><td><div class="align_right">' + callBackData.result[0].closeMarketValue + '</div></td><td><div class="align_right">' + callBackData.result[1].closeMarketValue + '</div></td><td><div class="align_right">' + callBackData.result[2].closeMarketValue + '</div></td></tr>');
                        arr.push('<tr><td>流通市值（万元）</td><td><div class="align_right">' + callBackData.result[0].closeNegoValue + '</div></td><td><div class="align_right">' + callBackData.result[1].closeNegoValue + '</div></td><td><div class="align_right">' + callBackData.result[2].closeNegoValue + '</div></td></tr>');
                        arr.push('<tr><td>成交量（' + showKCB + '）</td><td><div class="align_right">' + renum(callBackData.result[0].totalVol1, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].totalVol1, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].totalVol1, 2) + '</div></td></tr>');
                        arr.push('<tr><td>成交金额（万元）</td><td><div class="align_right">' + callBackData.result[0].totalAmt + '</div></td><td><div class="align_right">' + callBackData.result[1].totalAmt + '</div></td><td><div class="align_right">' + callBackData.result[2].totalAmt + '</div></td></tr>');
                        arr.push('<tr><td>成交笔数（万笔）</td><td><div class="align_right">' + renum(callBackData.result[0].totalTx, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].totalTx, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].totalTx, 2) + '</div></td></tr>');
                        arr.push('<tr><td>开盘价（元）</td><td><div class="align_right">' + renum(callBackData.result[0].openPrice, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].openPrice, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].openPrice, 2) + '</div></td></tr>');
                        arr.push('<tr><td>收盘价（元）</td><td><div class="align_right">' + renum(callBackData.result[0].closePrice, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].closePrice, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].closePrice, 2) + '</div></td></tr>');
                        arr.push('<tr><td>静态市盈率（倍）</td><td><div class="align_right">' + renum(callBackData.result[0].closeProfitRate, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].closeProfitRate, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].closeProfitRate, 2) + '</div></td></tr>');
                        arr.push('<tr><td>期间振幅（%）</td><td><div class="align_right">' + renum(callBackData.result[0].totalChange, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].totalChange, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].totalChange, 2) + '</div></td></tr>');
                        arr.push('<tr><td>涨跌幅（%）</td><td><div class="align_right">' + renum(callBackData.result[0].change, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].change, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].change, 2) + '</div></td></tr>');
                        arr.push('<tr><td>换手率</td><td><div class="align_right">' + renum(callBackData.result[0].totalExchRate, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].totalExchRate, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].totalExchRate, 2) + '</div></td></tr>');
                        arr.push('<tr><td>累计交易日</td><td><div class="align_right">-</div></td><td><div class="align_right">' + callBackData.result[1].totalTxDate + '</div></td><td><div class="align_right">' + callBackData.result[2].totalTxDate + '</div></td></tr>');
                        arr.push('<tr><td>最高价（元）</td><td><div class="align_right">' + renum(callBackData.result[0].maxHighPrice, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].maxHighPrice, 2) + '<br/>(' + ifNullTurn(callBackData.result[1].maxHighPriceDate) + ')</div></td><td><div class="align_right">' + renum(callBackData.result[2].maxHighPrice, 2) + '<br/>(' + ifNullTurn(callBackData.result[2].maxHighPriceDate) + ')</div></td></tr>');
                        arr.push('<tr><td>最低价（元）</td><td><div class="align_right">' + renum(callBackData.result[0].minLowPrice, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].minLowPrice, 2) + '<br/>(' + ifNullTurn(callBackData.result[1].minLowPriceDate) + ')</div></td><td><div class="align_right">' + renum(callBackData.result[2].minLowPrice, 2) + '<br/>(' + ifNullTurn(callBackData.result[2].minLowPriceDate) + ')</div></td></tr>');
                        arr.push('<tr><td>最高成交量（' + showKCB + '）</td><td><div class="align_right">-</div></td><td><div class="align_right">' + callBackData.result[1].maxTrVol1 + '<br/>(' + ifNullTurn(callBackData.result[1].maxTrVolDate) + ')</div></td><td><div class="align_right">' + callBackData.result[2].maxTrVol1 + '<br/>(' + ifNullTurn(callBackData.result[2].maxTrVolDate) + ')</div></td></tr>');
                        arr.push('<tr><td>最低成交量（' + showKCB + '）</td><td><div class="align_right">-</div></td><td><div class="align_right">' + callBackData.result[1].minTrVol1 + '<br/>(' + ifNullTurn(callBackData.result[1].minTrVolDate) + ')</div></td><td><div class="align_right">' + callBackData.result[2].minTrVol1 + '<br/>(' + ifNullTurn(callBackData.result[2].minTrVolDate) + ')</div></td></tr>');
                        arr.push('<tr><td>最高成交金额（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">' + callBackData.result[1].maxTrAmt + '<br/>(' + ifNullTurn(callBackData.result[1].maxTrAmtDate) + ')</div></td><td><div class="align_right">' + callBackData.result[2].maxTrAmt + '<br/>(' + ifNullTurn(callBackData.result[2].maxTrAmtDate) + ')</div></td></tr>');
                        arr.push('<tr><td>最低成交金额（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">' + callBackData.result[1].minTrAmt + '<br/>(' + ifNullTurn(callBackData.result[1].minTrAmtDate) + ')</div></td><td><div class="align_right">' + callBackData.result[2].minTrAmt + '<br/>(' + ifNullTurn(callBackData.result[2].minTrAmtDate) + ')</div></td></tr>');
                    } else {
                        arr.push('<tr><td>市价总值（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>流通市值（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>成交量（' + showKCB + '）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>成交金额（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>成交笔数（万笔）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>开盘价（元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>收盘价（元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>静态市盈率（倍）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>期间振幅（%）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>涨跌幅（%）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>换手率</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>累计交易日</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最高价（元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最低价（元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最高成交量（' + showKCB + '）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最低成交量（' + showKCB + '）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最高成交金额（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最低成交金额（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                    }
                    $(".table").eq(0).html(arr.join(""));
                    hideloading()
                }
            });
        }
        if (codeB.length == 6) {
            //成交概况表格
            showloading();
            $.ajax({
                url: sseQueryURL + "security/fund/queryNewAllQuatAbel.do",
                type: "post",
                dataType: "jsonp",
                jsonp: "jsonCallBack",
                jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
                data: {
                    "FUNDID": codeB,
                    "inMonth": obj.inMonth,
                    "inYear": obj.inYear,
                    "searchDate": obj.searchDate
                },
                success: function(callBackData) {
                    var arr = [];
                    if (callBackData.result.length > 0) {
                        arr.push('<tr><th></th><th>' + obj.searchDate.substring(0, 4) + '年' + obj.searchDate.substring(5, 7) + '月' + obj.searchDate.substring(8, 10) + '日</th><th>' + obj.inMonth.substring(0, 4) + '年' + obj.inMonth.substring(4, 6) + '月</th><th>' + obj.inYear + '年</th></tr>');
                        arr.push('<tr><td>市价总值（万元）</td><td><div class="align_right">' + callBackData.result[0].closeMarketValue + '</div></td><td><div class="align_right">' + callBackData.result[1].closeMarketValue + '</div></td><td><div class="align_right">' + callBackData.result[2].closeMarketValue + '</div></td></tr>');
                        arr.push('<tr><td>流通市值（万元）</td><td><div class="align_right">' + callBackData.result[0].closeNegoValue + '</div></td><td><div class="align_right">' + callBackData.result[1].closeNegoValue + '</div></td><td><div class="align_right">' + callBackData.result[2].closeNegoValue + '</div></td></tr>');
                        arr.push('<tr><td>成交量（' + showKCB + '）</td><td><div class="align_right">' + renum(callBackData.result[0].totalVol1, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].totalVol1, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].totalVol1, 2) + '</div></td></tr>');
                        arr.push('<tr><td>成交金额（万元）</td><td><div class="align_right">' + callBackData.result[0].totalAmt + '</div></td><td><div class="align_right">' + callBackData.result[1].totalAmt + '</div></td><td><div class="align_right">' + callBackData.result[2].totalAmt + '</div></td></tr>');
                        arr.push('<tr><td>成交笔数（万笔）</td><td><div class="align_right">' + renum(callBackData.result[0].totalTx, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].totalTx, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].totalTx, 2) + '</div></td></tr>');
                        arr.push('<tr><td>开市价</td><td><div class="align_right">' + renum(callBackData.result[0].openPrice, 3) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].openPrice, 3) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].openPrice, 3) + '</div></td></tr>');
                        arr.push('<tr><td>收市价</td><td><div class="align_right">' + renum(callBackData.result[0].closePrice, 3) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].closePrice, 3) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].closePrice, 3) + '</div></td></tr>');
                        arr.push('<tr><td>静态市盈率（倍）</td><td><div class="align_right">' + renum(callBackData.result[0].closeProfitRate, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].closeProfitRate, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].closeProfitRate, 2) + '</div></td></tr>');
                        arr.push('<tr><td>期间振幅（%）</td><td><div class="align_right">' + renum(callBackData.result[0].totalChange, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].totalChange, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].totalChange, 2) + '</div></td></tr>');
                        arr.push('<tr><td>涨跌幅（%）</td><td><div class="align_right">' + renum(callBackData.result[0].change, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].change, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].change, 2) + '</div></td></tr>');
                        arr.push('<tr><td>换手率</td><td><div class="align_right">' + renum(callBackData.result[0].totalExchRate, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].totalExchRate, 2) + '</div></td><td><div class="align_right">' + renum(callBackData.result[2].totalExchRate, 2) + '</div></td></tr>');
                        arr.push('<tr><td>累计交易日</td><td><div class="align_right">-</div></td><td><div class="align_right">' + callBackData.result[1].totalTxDate + '</div></td><td><div class="align_right">' + callBackData.result[2].totalTxDate + '</div></td></tr>');
                        arr.push('<tr><td>最高成价（元）</td><td><div class="align_right">' + renum(callBackData.result[0].maxHighPrice, 3) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].maxHighPrice, 3) + '<br/>(' + ifNullTurn(callBackData.result[1].maxHighPriceDate) + ')</div></td><td><div class="align_right">' + renum(callBackData.result[2].maxHighPrice, 3) + '<br/>(' + ifNullTurn(callBackData.result[2].maxHighPriceDate) + ')</div></td></tr>');
                        arr.push('<tr><td>最低成价（元）</td><td><div class="align_right">' + renum(callBackData.result[0].minLowPrice, 3) + '</div></td><td><div class="align_right">' + renum(callBackData.result[1].minLowPrice, 3) + '<br/>(' + ifNullTurn(callBackData.result[1].minLowPriceDate) + ')</div></td><td><div class="align_right">' + renum(callBackData.result[2].minLowPrice, 3) + '<br/>(' + ifNullTurn(callBackData.result[2].minLowPriceDate) + ')</div></td></tr>');
                        arr.push('<tr><td>最高成交量（' + showKCB + '）</td><td><div class="align_right">-</div></td><td><div class="align_right">' + callBackData.result[1].maxTrVol1 + '<br/>(' + ifNullTurn(callBackData.result[1].maxTrVolDate) + ')</div></td><td><div class="align_right">' + callBackData.result[2].maxTrVol1 + '<br/>(' + ifNullTurn(callBackData.result[2].maxTrVolDate) + ')</div></td></tr>');
                        arr.push('<tr><td>最低成交量（' + showKCB + '）</td><td><div class="align_right">-</div></td><td><div class="align_right">' + callBackData.result[1].minTrVol1 + '<br/>(' + ifNullTurn(callBackData.result[1].minTrVolDate) + ')</div></td><td><div class="align_right">' + callBackData.result[2].minTrVol1 + '<br/>(' + ifNullTurn(callBackData.result[2].minTrVolDate) + ')</div></td></tr>');
                        arr.push('<tr><td>最高成交金额（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">' + callBackData.result[1].maxTrAmt + '<br/>(' + ifNullTurn(callBackData.result[1].maxTrAmtDate) + ')</div></td><td><div class="align_right">' + callBackData.result[2].maxTrAmt + '<br/>(' + ifNullTurn(callBackData.result[2].maxTrAmtDate) + ')</div></td></tr>');
                        arr.push('<tr><td>最低成交金额（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">' + callBackData.result[1].minTrAmt + '<br/>(' + ifNullTurn(callBackData.result[1].minTrAmtDate) + ')</div></td><td><div class="align_right">' + callBackData.result[2].minTrAmt + '<br/>(' + ifNullTurn(callBackData.result[2].minTrAmtDate) + ')</div></td></tr>');
                    } else {
                        arr.push('<tr><td>市价总值（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>流通市值（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>成交量（' + showKCB + '）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>成交金额（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>成交笔数（万笔）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>开市价</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>收市价</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>静态市盈率（倍）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>期间振幅（%）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>涨跌幅（%）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>换手率</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>累计交易日</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最高成价（元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最低成价（元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最高成交量（' + showKCB + '）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最低成交量（' + showKCB + '）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最高成交金额（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                        arr.push('<tr><td>最低成交金额（万元）</td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td><td><div class="align_right">-</div></td></tr>');
                    }
                    $(".table").eq(1).html(arr.join(""));
                    hideloading()
                }
            });
        }
        if (type == 1) {
            $(".nav-tabs").find("li").eq(1).hide();
            $(".nav-tabs").find("li").eq(0).show();
            $(".nav-tabs").find("li").eq(1).removeClass("active");
            $(".nav-tabs").find("li").eq(0).addClass("active");
            $(".js_tableT01").eq(1).removeClass("active");
            $(".js_tableT01").eq(0).addClass("active");
        } else if (type == 2) {
            $(".nav-tabs").find("li").eq(0).hide();
            $(".nav-tabs").find("li").eq(1).show();
            $(".nav-tabs").find("li").eq(0).removeClass("active");
            $(".nav-tabs").find("li").eq(1).addClass("active");
            $(".js_tableT01").eq(0).removeClass("active");
            $(".js_tableT01").eq(1).addClass("active");
        } else if (type == 3) {
            $(".nav-tabs").find("li").eq(0).show();
            $(".nav-tabs").find("li").eq(1).show();
            $(".nav-tabs").find("li").eq(1).removeClass("active");
            $(".nav-tabs").find("li").eq(0).addClass("active");
        }
    }

    if (urlProductid != null && urlProductid != "" && urlProductid != undefined) {
        fakeLabelChoice(urlProductid);
        /*
        获取标题
         */
        $.ajax({
            url: sseQueryURL + 'commonQuery.do',
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            data: {
                'isPagination': false,
                'sqlId': 'COMMON_SSE_ZQPZ_GP_GPLB_C',
                'productid': urlProductid
            },
            success: function(callBackData) {
                //将修改成table展示
                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                    var FULLNAME = callBackData.result[0].FULLNAME;
                    $(".sse_list_tit1").find("span").html(callBackData.result[0].FULLNAME + urlProductid);
                    if (callBackData.result[0].TYPE != '0') {
                        // isKcb = true;
                        // showAshare = '科创板A股';
                        showBshare = '科创板CDR';
                        if (callBackData.result[0].TYPE == '1') {
                            showAshare = '科创板A股';
                        } else if (callBackData.result[0].TYPE == '2') {
                            showAshare = '科创板CDR';
                        }
                        showKCB = '万股/万份';
                    } else {
                        // isKcb = false;
                        showAshare = 'A股';
                        showBshare = 'B股';
                        showKCB = '万股';
                    }
                    $(".nav-tabs").find("li").eq(0).find('a').html(showAshare);
                    $(".nav-tabs").find("li").eq(1).find('a').html(showBshare);
                    codeA = callBackData.result[0].SECURITY_CODE_A;
                    codeB = callBackData.result[0].SECURITY_CODE_B;
                    if (codeA.length == 6 && codeB.length == 6) {
                        type = 3;
                    } else if (codeA.length == 6 && codeB.length != 6) {
                        type = 1;
                    } else if (codeA.length != 6 && codeB.length == 6) {
                        type = 2;
                    }

                    //三个参数拼装
                    //var searchDay = get_lastTradeDate_global();
                    var searchDay = "2022-01-07";
                    $("#start_date2").val(searchDay);

                    var iny = Number(searchDay.substring(0, 4));
                    var searchYear = Number(searchDay.substring(0, 4)) - 1;
                    var inMonth = Number(searchDay.substring(5, 7)) - 1;

                    if (inMonth == 0) {
                        inMonth = 12;
                        iny--;
                    }
                    if (inMonth <= 9) {
                        inMonth = "0" + inMonth;
                    };

                    //方法调用
                    doShowTableStockOver({
                        "inMonth": iny + "" + inMonth,
                        "inYear": searchYear,
                        "searchDate": searchDay
                    });
                }
            }
        });

        //查询按钮绑定
        $qbuttonStockViewOver.on("click", function() {
            var searchDate = $("#start_date2").val();
            if (searchDate == "查询日期") {
                searchDate = "";
            }
            if (searchDate == "" || searchDate == "查询日期") {
                alert("请输入查询日期。");
            } else if (searchDate < "1990-12-19") {
                alert("您选择的日期超出检索范围，请重输。");
            } else if (searchDate > "2022-01-07") {
                //alert("您选择的日期超出检索范围，请重输。");
                $(".table").eq(0).html('<tr><th><a href="http://www.sse.com.cn/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=' + urlProductid + '">更多数据，点击此处。</a></th></tr>');
            } else {
                //执行查询方法
                doShowTableStockOver({
                    "inMonth": searchDate.replace(/\-/g, "").substring(0, 6),
                    "inYear": searchDate.replace(/\-/g, "").substring(0, 4),
                    "searchDate": searchDate
                });
            }
        });
    }
}

//===================股票概况-股本结构================================
var $StockViewTwo = $(".search_stockViewTwo");
if ($StockViewTwo.length > 0) {
    var urlProductid = GetQueryString("COMPANY_CODE");
    var isGbjgKcb;
    var stockTypeDw = '万股';
    /*
     *  数据展示
     */
    function doShowStockViewTwo(callBackData) {
        var arr = [];
        arr.push('<tr>');
        arr.push('<th>股份名称</th>');
        arr.push('<th>发行总股本(' + stockTypeDw + ')</th>');
        // arr.push('<th>比例(%)</th>');
        // arr.push('<th>较上月增减(万股)</th>');
        arr.push('</tr>');

        var arrA = callBackData.result[0];
        if (arrA != "" && arrA != null) {
            $("#tableData_one").find(".sse_table_title2").show();
            $("#tableData_two").find(".sse_table_title2").show();
            if (arrA.REAL_DATE) {
                var endDate = arrA.REAL_DATE.replace("-", "年");
                $("#tableData_one").find(".sse_table_title2").find("p").html("数据日期：" + endDate.replace("-", "月") + "日");
                $("#tableData_two").find(".sse_table_title2").find("p").html("数据日期：" + endDate.replace("-", "月") + "日");
            }
            arr.push('<tr><td>有限售流通股</td><td ><div class="align_right">' + ifundefindTurn1(arrA.LIMITED_SHARES) + '</div></td></tr>');
            arr.push('<tr><td style="text-indent:1em;">其中：特别表决权股</td><td><div class="align_right">' + ifundefindTurn1(arrA.LISTING_VOTE_SHARES) + '</div></td></tr>');
            arr.push('<tr><td>无限售流通股</td><td><div class="align_right">' + ifundefindTurn1(arrA.UNLIMITED_SHARES) + '</div></td></tr>');
            arr.push('<tr><td style="text-indent:1em;">无限售流通A股/CDR</td><td><div class="align_right">' + ifundefindTurn1(arrA.UNLIMITED_A_SHARES) + '</div></td></tr>');
            arr.push('<tr><td style="text-indent:1em;">境内上市外资股（B股）</td><td><div class="align_right">' + ifundefindTurn1(arrA.B_SHARES) + '</div></td></tr>');
            arr.push('<tr><td>境内上市股票合计</td><td><div class="align_right">' + ifundefindTurn1(arrA.DOMESTIC_SHARES) + '</div></td></tr>');
        } else {
            arr.push('<tr><td>有限售流通股</td><td ><div class="align_right">-</div></td></tr>');
            arr.push('<tr><td style="text-indent:1em;">其中：特别表决权股</td><td><div class="align_right">-</div></td></tr>');
            arr.push('<tr><td>无限售流通股</td><td><div class="align_right">-</div></td></tr>');
            arr.push('<tr><td style="text-indent:1em;">无限售流通A股/CDR</td><td><div class="align_right">-</div></td></tr>');
            arr.push('<tr><td style="text-indent:1em;">境内上市外资股（B股）</td><td><div class="align_right">-</div></td></tr>');
            arr.push('<tr><td>境内上市股票合计</td><td><div class="align_right">-</div></td></tr>');
        }
        //设置表内容
        $('table').eq(0).html(arr.join(""));
    }

    function showajaxStockViewTwoNew(obj) {
        var tempData = {
                isPageing: true,
                url: sseQueryURL + 'security/stock/queryEquityChangeAndReason.do',
                params: {
                    "isPagination": true,
                    "companyCode": obj.companyCode,
                    "pageHelp.pageSize": sitePageSize,
                    "pageHelp.pageCount": 50,
                    "pageHelp.pageNo": 1,
                    "pageHelp.beginPage": 1,
                    "pageHelp.cacheSize": 1,
                    "pageHelp.endPage": 5
                }
            }
            // var stockIsKcb1 = obj.isKcb

        var ajaxSearch01 = function(tableData, obj, results, pageCache) {
            // var stockIsKcb1;


            var arr = [];
            arr.push('<tr>');
            arr.push('<th>变动日期</th>');
            arr.push('<th>变动原因</th>');
            arr.push('<th>变动后股数(' + stockTypeDw + ')</th>');
            // if (!stockIsKcb1) {
            arr.push('<th></th>');
            // }

            arr.push('</tr>');

            var arrA = results.dataJson;
            if (arrA != "" && arrA != null && arrA != undefined) {
                for (var i = 0; i < arrA.length; ++i) {
                    var data = arrA[i];
                    arr.push('<tr><td>' + data.realDate + '</td><td>' + data.changeReasonDesc + '</td><td><div class="align_right">' + data.totalShares.toFixed(2) + '</div></td>');
                    // if (!stockIsKcb1) {
                    arr.push('<td><a  target="_blank" href="/assortment/stock/list/info/capital/detailIndex.shtml?COMPANY_CODE=' + urlProductid + '&seq=' + data.seq + '">详情</a></td>');
                    // }
                    arr.push('</tr>')
                }
            } else {
                arr.push("<tr><td colspan='50'>未找到，只有0条数据！</td></tr>");
            }
            //设置表内容
            $('table').eq(1).html(arr.join(""));
        }

        if (tempData.isPageing) {
            loadPage(tempData, {
                pageSelect: $('#tableData_two')
            }, ajaxSearch01);
        }
    }
    /*
     *  数据展示
     */
    function doShowStockViewTwoB(callBackData) {
        var arr = [];
        arr.push('<tr>');
        arr.push('<th>变动日期</th>');
        arr.push('<th>变动原因</th>');
        arr.push('<th>变动后股数(' + stockTypeDw + ')</th>');
        arr.push('<th></th>');
        arr.push('</tr>');

        var arrA = callBackData.result;
        if (arrA != "" && arrA != null && arrA != undefined) {
            for (var i = 0; i < arrA.length; ++i) {
                var data = arrA[i];
                arr.push('<tr><td>' + data.realDate + '</td><td>' + data.changeReasonDesc + '</td><td><div class="align_right">' + data.totalShares.toFixed(2) + '</div></td>');
                arr.push('<td><a  target="_blank" href="/assortment/stock/list/info/capital/detailIndex.shtml?COMPANY_CODE=' + urlProductid + '&seq=' + data.seq + '">详情</a></td></tr>');
            }
        } else {
            arr.push("<tr><td colspan='50'>未找到，只有0条数据！</td></tr>");
        }
        //设置表内容
        $('table').eq(1).html(arr.join(""));
    }
    /*
     *ajax请求
     */
    function showajaxStockViewTwo(jsonObj) {
        $.ajax({
            url: jsonObj.url,
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: jsonObj.param,

            success: function(callBackData) {
                doShowStockViewTwo(callBackData);
            }
        });
    }

    /*
     *ajax请求
     */
    function showajaxStockViewTwoB(jsonObj) {
        $.ajax({
            url: jsonObj.url,
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: jsonObj.param,

            success: function(callBackData) {
                doShowStockViewTwoB(callBackData);
            }
        });
    }

    if (urlProductid != null && urlProductid != "" && urlProductid != undefined) {

        /*
        获取标题
         */
        $.ajax({
            url: sseQueryURL + 'commonQuery.do',
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: {
                'isPagination': false,
                'sqlId': 'COMMON_SSE_ZQPZ_GP_GPLB_C',
                'productid': urlProductid
            },
            success: function(callBackData) {
                //将修改成table展示
                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                    $(".sse_list_tit1").find("span").html(callBackData.result[0].FULLNAME + urlProductid);
                    fakeLabelChoice(urlProductid);
                    if (callBackData.result[0].TYPE != 0) {
                        stockTypeDw = '万股/万份';
                    }
                    showajaxStockViewTwo({
                        url: sseQueryURL + 'commonQuery.do',

                        param: {
                            isPagination: false,
                            sqlId: 'COMMON_SSE_CP_GPLB_GPGK_GBJG_C',
                            companyCode: urlProductid
                        }
                    });

                    //请求ajax
                    // stockIsKcb(urlProductid, function(isKcb) {
                    showajaxStockViewTwoNew({
                        "isPagination": true,
                        "companyCode": urlProductid,
                        // "isKcb": isKcb
                    });
                    // isGbjgKcb = callBackData.result[0].TYPE;
                }
            }

        });

        //请求ajax

        // })

        /*
        showajaxStockViewTwoB({
        url : sseQueryURL + 'security/stock/queryEquityChangeAndReason.do',
        param : {
        isPagination : true,
        companyCode : urlProductid
        }
        });
         */

    } else {
        doShowStockViewTwo("");
        doShowStockViewTwoB("");
    }
}

// 盘中临时停牌
var $pzlstp_kcb = $(".search_pzlstp_kcb");
if ($pzlstp_kcb.length > 0) {
    $.ajax({
        url: sseQueryURL + 'commonQuery.do',
        type: "post",
        dataType: "jsonp",
        jsonp: "jsonCallBack",
        data: {
            'isPagination': true,
            'sqlId': 'COMMON_STOCK_PZTP_BULLETIN_LB',
            'pageHelp.pageSize': 1000
        },
        success: function(callBackData) {
            var showHtml = [];

            //将修改成table展示
            if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                var data = callBackData.result;
                for (var i = 0; i < data.length; i++) {
                    var _index = data[i].NUM;
                    var _title = data[i].TITLE;
                    var _issue = data[i].ISSUE_NUM;
                    var _contentArr = data[i].CONTENT.split('\n');
                    var content = '';
                    for (var j = 0; j < _contentArr.length; j++) {
                        content += '<p style="text-indent:2em">' + _contentArr[j] + '</p>';
                    }
                    showHtml.push('<tr><td style="text-align: center;">' + _index + '</td>');
                    showHtml.push('<td><div style="text-align: center;"><p>' + _title + '</p></div><div style="text-align: center;"><p>' + _issue + '</p></div><div>' + content + '</div></td></tr>');
                }
            } else {
                showHtml.push('<tr><td>1</td><td>暂无数据</td></tr>');
            }
            $pzlstp_kcb.find('table').append(showHtml.join(""));
        }
    });
}

// 主板盘中停牌提示
var $zbpz = $('.search_pztpts_zb');
if ($zbpz.length > 0) {
    $.ajax({
        url: sseQueryURL + 'commonQuery.do',
        type: "post",
        dataType: "jsonp",
        jsonp: "jsonCallBack",
        jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
        data: {
            'isPagination': true,
            "sqlId": "COMMON_STOCK_ZBPZTP_BULLETIN_LB",
            'pageHelp.pageSize': 1000
        },
        success: function(callBackData) {
            var showHtml = [];
            // showHtml.push('<tbody><tr><th>序号</th><th>内容</th></tr>');
            // if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
            //   var data = callBackData.result;
            //   for (var i = 0; i < data.length; i++) {
            //     var _index = data[i].NUM;
            //     var _title = data[i].TITLE;
            //     var _issue = data[i].ISSUE_NUM;
            //     var _contentArr = data[i].CONTENT.split('\n');
            //     var content = '';
            //     for (var j = 0; j < _contentArr.length; j++) {
            //       content += '<p style="text-indent:2em">' + _contentArr[j] + '</p>';
            //     }
            //     showHtml.push('<tr><td style="text-align: center;">' + _index + '</td>');
            //     showHtml.push('<td><div style="text-align: center;"><p>' + _title + '</p></div><div style="text-align: center;"><p>' + _issue + '</p></div><div>' + content + '</div></td></tr>');
            //   }
            // } else {
            //   showHtml.push('<tr><td>1</td><td>暂无数据</td></tr>');
            // }

            var _index = $zbpz.find('.table tr:last-child .td_text_center span').text()
            if (_index == '') {
                $zbpz.find('.table tbody tr').eq(1).remove()
            }
            if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                var data = callBackData.result;
                for (var i = 0; i < data.length; i++) {
                    var _title = data[i].TITLE;
                    var _issue = data[i].ISSUE_NUM;
                    var _contentArr = data[i].CONTENT.split('\n');
                    var content = '';
                    for (var j = 0; j < _contentArr.length; j++) {
                        content += '<p style="text-indent:2em">' + _contentArr[j] + '</p>';
                    }
                    showHtml.push('<tr><td style="text-align: center;">' + (++_index) + '</td>');
                    showHtml.push('<td><div style="text-align: center;"><p>' + _title + '</p></div><div style="text-align: center;"><p>' + _issue + '</p></div><div>' + content + '</div></td></tr>');
                }

                $zbpz.find('.table tbody').append(showHtml.join(""));
            } else if (_index == '') {
                $zbpz.find('.table tbody tr').eq(1).remove()
                showHtml.push('<tr><td>1</td><td>暂无数据</td></tr>');
                $zbpz.find('.table tbody').append(showHtml.join(""));
            }

        }
    });
}

var $jssearchGGGBJG = $(".search_gggbjg");
if ($jssearchGGGBJG.length) {

    var inputCode = $("#inputCode").val();

    var $qbuttonZCGLZR = $jssearchGGGBJG.find(".btn-primary");
    //按钮点击事件绑定
    $qbuttonZCGLZR.on("click", function() {

        var inputCode = $("#inputCode").val();
        var url = '/assortment/stock/list/info/capital/index.shtml?COMPANY_CODE=' + inputCode;
        windowOpen(url);
    });

}


//股票数据总貌 股票规模 
//数据总貌
var $tableZm = $(".js_tableZm");
if ($tableZm.length) {
    var strArr = [];

    strArr.push('<div class="sse_home_in_table2">');

    strArr.push('<table class="table hidden-xs"><tbody>');
    strArr.push('<tr><td style="width: 25%;"><i>上市公司/家</i><em>' + home_sjtj.companyNumber + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>上市股票/只</i><em>' + home_sjtj.stockNumber + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>总股本/亿股（份）</i><em>' + home_sjtj.iss_vol + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>流通股本/亿股（份）</i><em>' + home_sjtj.ngt_vol + '</em></td></tr>');
    strArr.push('<tr><td style="width: 25%;"><i>总市值/亿元</i><em>' + home_sjtj.mkt_value + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>流通市值/亿元</i><em>' + home_sjtj.negotiable_value + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>平均市盈率/倍</i><em>' + home_sjtj.ratioOfPe + '</em></td></tr>');
    strArr.push('</tbody></table>');
    strArr.push('<table class="table visible-xs"><tbody>');
    strArr.push('<tr><td><i>上市公司/家</i><em>' + home_sjtj.companyNumber + '</em></td>');
    strArr.push('<td><i>上市股票/只</i><em>' + home_sjtj.stockNumber + '</em></td></tr>');
    strArr.push('<tr><td><i>总股本/亿股（份）</i><em>' + home_sjtj.iss_vol + '</em></td>');
    strArr.push('<td><i>流通股本/亿股（份）</i><em>' + home_sjtj.ngt_vol + '</em></td></tr>');
    strArr.push('<tr><td><i>总市值/亿元</i><em>' + home_sjtj.mkt_value + '</em></td>');
    strArr.push('<td><i>流通市值/亿元</i><em>' + home_sjtj.negotiable_value + '</em></td></tr>');
    strArr.push('<tr><td><i>平均市盈率/倍</i><em>' + home_sjtj.ratioOfPe + '</em></td></tr>');
    strArr.push('</tbody></table>');
    $tableZm.append(strArr.join(""));
    $('.statistical_data').css('border', 'none');
    var tjsjDate = '<div class="sse_table_title2"><p>数据日期：' + home_sjtj.dataStatisticDate + '</p></div>';
    $('.js_tjsjDate').prepend(tjsjDate);


}
var $tableZb = $(".js_tableZb");
if ($tableZb.length) {
    var strArr = [];

    strArr.push('<div class="sse_home_in_table2">');
    strArr.push('<table class="table hidden-xs"><tbody>');
    strArr.push('<tr><td style="width: 25%;"><i>上市公司/家</i><em>' + home_sjtj_zb.companyNumber + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>上市股票/只</i><em>' + home_sjtj_zb.stockNumber + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>总股本/亿股</i><em>' + home_sjtj_zb.iss_vol + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>流通股本/亿股</i><em>' + home_sjtj_zb.ngt_vol + '</em></td></tr>');
    strArr.push('<tr><td style="width: 25%;"><i>总市值/亿元</i><em>' + home_sjtj_zb.mkt_value + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>流通市值/亿元</i><em>' + home_sjtj_zb.negotiable_value + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>平均市盈率/倍</i><em>' + home_sjtj_zb.ratioOfPe + '</em></td></tr>');
    strArr.push('</tbody></table>');
    strArr.push('<table class="table visible-xs"><tbody>');
    strArr.push('<tr><td><i>上市公司/家</i><em>' + home_sjtj_zb.companyNumber + '</em></td>');
    strArr.push('<td><i>上市股票/只</i><em>' + home_sjtj_zb.stockNumber + '</em></td></tr>');
    strArr.push('<tr><td><i>总股本/亿股</i><em>' + home_sjtj_zb.iss_vol + '</em></td>');
    strArr.push('<td><i>流通股本/亿股</i><em>' + home_sjtj_zb.ngt_vol + '</em></td></tr>');
    strArr.push('<tr><td><i>总市值/亿元</i><em>' + home_sjtj_zb.mkt_value + '</em></td>');
    strArr.push('<td><i>流通市值/亿元</i><em>' + home_sjtj_zb.negotiable_value + '</em></td></tr>');
    strArr.push('<tr><td><i>平均市盈率/倍</i><em>' + home_sjtj_zb.ratioOfPe + '</em></td></tr>');
    strArr.push('</tbody></table>');
    $tableZb.append(strArr.join(""));
    $('.statistical_data').css('border', 'none');

}

var $tableKcb = $(".js_tableKcb");
if ($tableKcb.length) {
    var strArr = [];

    strArr.push('<div class="sse_home_in_table2">');

    strArr.push('<table class="table hidden-xs"><tbody>');
    strArr.push('<tr><td style="width: 25%;"><i>上市公司/家</i><em>' + home_sjtj_kcb.companyNumber + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>上市股票/只</i><em>' + home_sjtj_kcb.stockNumber + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>总股本/亿股（份）</i><em>' + home_sjtj_kcb.iss_vol + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>流通股本/亿股（份）</i><em>' + home_sjtj_kcb.ngt_vol + '</em></td></tr>');
    strArr.push('<tr><td style="width: 25%;"><i>总市值/亿元</i><em>' + home_sjtj_kcb.mkt_value + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>流通市值/亿元</i><em>' + home_sjtj_kcb.negotiable_value + '</em></td>');
    strArr.push('<td style="width: 25%;"><i>平均市盈率/倍</i><em>' + home_sjtj_kcb.ratioOfPe + '</em></td></tr>');
    strArr.push('</tbody></table>');
    strArr.push('<table class="table visible-xs"><tbody>');
    strArr.push('<tr><td><i>上市公司/家</i><em>' + home_sjtj_kcb.companyNumber + '</em></td>');
    strArr.push('<td><i>上市股票/只</i><em>' + home_sjtj_kcb.stockNumber + '</em></td></tr>');
    strArr.push('<tr><td><i>总股本/亿股（份）</i><em>' + home_sjtj_kcb.iss_vol + '</em></td>');
    strArr.push('<td><i>流通股本/亿股（份）</i><em>' + home_sjtj_kcb.ngt_vol + '</em></td></tr>');
    strArr.push('<tr><td><i>总市值/亿元</i><em>' + home_sjtj_kcb.mkt_value + '</em></td>');
    strArr.push('<td><i>流通市值/亿元</i><em>' + home_sjtj_kcb.negotiable_value + '</em></td></tr>');
    strArr.push('<tr><td><i>平均市盈率/倍</i><em>' + home_sjtj_kcb.ratioOfPe + '</em></td></tr>');
    strArr.push('</tbody></table>');

    $tableKcb.append(strArr.join(""));
    $('.statistical_data').css('border', 'none');

}

var $searchStockCode = $(".search_stockCode");
if ($searchStockCode.length) {
    var $qbuttonStockCode = $searchStockCode.find("#btnQuery");
    $qbuttonStockCode.on("click", function() {
        var stockCode = $('#inputCode').val();
        if (stockCode == "" || stockCode == "证券代码或简称") {
            alert("请输入证券代码或简称");
        } else if (isNaN(stockCode)) {
            alert("证券代码必须为6位数字，请根据智能提示选择证券代码后查询");
        } else if (stockCode.length != 6) {
            alert("证券代码必须为6位数字");
        } else {
            location.href = '/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=' + stockCode;
        }
    });
}


//================股票概况-筹资情况-new====================
var $StockViewFinanNew = $("#tableData_stockListFirDay");
if ($StockViewFinanNew.length > 0) {
    $('.sse_common_second_cn').hide();
    var codeA = "";
    var codeB = "";
    var issueMark = ''; //新增 首发类型  1：A股首发    10：科创板首发   11：科创板CDR首发
    var _type = 'inParams';
    var urlProductid = GetQueryString("COMPANY_CODE");
    var stockTypeDw = '万股';
    var showA_1, showA_2, showA_3, gpsf_sid, gpzf_sid, gppg_sid, isKcb, stockType;
    var htm = "<a href='/assortment/stock/list/info/company/index.shtml?COMPANY_CODE=" + urlProductid +
        "' style='font-size:12px;font-weight:normal;float:inherit' target ='blank' title='数据截止到2022年1月9日'>(此栏目为历史数据，更多数据点击此处)</a>"
    $(".sse_title_common").eq(0).find("h2").html("首日表现" + htm);
    if (urlProductid != null && urlProductid != "" && urlProductid != undefined) {
        /*
        获取标题
         */
        $.ajax({
            url: sseQueryURL + 'commonQuery.do',
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: {
                'isPagination': false,
                'sqlId': 'COMMON_SSE_ZQPZ_GP_GPLB_C',
                'productid': urlProductid
            },
            success: function(callBackData) {
                //将修改成table展示
                stockType = callBackData.result[0].TYPE;
                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                    var FULLNAME = callBackData.result[0].FULLNAME;

                    if (callBackData.result[0].TYPE != '0') {
                        showA_1 = '科创板首次发行';
                        showB_2 = '科创板增发';
                        showB_3 = '科创板配股';
                        issueMark = '1,11';
                        gpsf_sid = 'COMMON_SSE_ZQPZ_GPLB_CZQK_SCFX_C'; //COMMON_SSE_ZQPZ_GPLB_CZQK_KCBSCFX_S
                        gpzf_sid = 'COMMON_SSE_ZQPZ_GPLB_CZQK_KCBZF_S';
                        gppg_sid = 'COMMON_SSE_ZQPZ_GPLB_CZQK_KCBPG_S';
                        stockTypeDw = '万股/万份';
                        isKcb = true;

                    } else {
                        showA_1 = 'A首次发行';
                        showB_2 = 'A增发';
                        showB_3 = 'A配股';
                        issueMark = '1,11';
                        gpsf_sid = 'COMMON_SSE_ZQPZ_GPLB_CZQK_SCFX_C'; //COMMON_SSE_ZQPZ_GPLB_CZQK_AGSCFX_S
                        gpzf_sid = 'COMMON_SSE_ZQPZ_GPLB_CZQK_AGZF_S';
                        gppg_sid = 'COMMON_SSE_ZQPZ_GPLB_CZQK_AGPG_S';
                        stockTypeDw = '万股';
                        isKcb = false;
                    }
                    // $(".nav-tabs").eq(0).find("li").eq(0).find('a').html(showA_1);
                    // $(".nav-tabs").eq(0).find("li").eq(1).find('a').html(showB_2);
                    // $(".nav-tabs").eq(0).find("li").eq(2).find('a').html(showB_3);
                    codeA = callBackData.result[0].SECURITY_CODE_A;
                    codeB = callBackData.result[0].SECURITY_CODE_B;

                    /* if (codeA != '' && codeA != '-' && codeA != undefined && codeA != null) {
                      asf(isKcb);
                      azf(isKcb);
                      apg(isKcb);
                    }
                    if (codeB != '' && codeB != '-' && codeB != undefined && codeB != null) {
                      bsf();
                      bzf();
                      bpg();
                    } */
                    tssj();
                    $(".sse_list_tit1").find("span").html(callBackData.result[0].FULLNAME + urlProductid);
                    fakeLabelChoice(urlProductid);
                }
            }
        });


    }
    //【A股及科创板首次发行】
    function asf(isKcb) {
        var $ele = isKcb ? $('.sse_common_second_cn').eq(6) : $('.sse_common_second_cn').eq(0);
        $.ajax({
            url: sseQueryURL + 'commonQuery.do',
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: {
                'isPagination': false,
                'sqlId': gpsf_sid,
                'productid': codeA,
                //新增 start
                'issueMark': issueMark,
                'type': _type
                    //新增 end 
            },
            success: function(callBackData) {
                //将修改成table展示
                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                    var arr = [];
                    if (stockType != "0") {
                        arr.push('<tr><th rowspan="2">发行数<br/>量(' + stockTypeDw + ')</th><th rowspan="2">其中：战投获配<br/>数量(' + stockTypeDw + ')</th><th rowspan="2">发行<br/>价格</th><th rowspan="2">发行日期</th><th rowspan="2">募集资金<br/>总额(万元)</th><th colspan="2">发行市盈率(%)</th><th rowspan="2">发行方式</th><th rowspan="2">主承销商</th><th rowspan="2">中签<br>率%</th></tr>');
                        arr.push('<tr><th>加权<br>法</th><th>摊薄<br>法 </th></tr>');
                    }
                    if (stockType == "0") {
                        arr.push('<tr><th rowspan="2">发行数<br/>量(万股)</th><th rowspan="2">发行<br/>价格</th><th rowspan="2">发行日期</th><th rowspan="2">募集资金<br/>总额(万元)</th><th colspan="2">发行市盈率(%)</th><th rowspan="2">发行方式</th><th rowspan="2">主承销商</th><th rowspan="2">中签<br>率%</th></tr>');
                        arr.push('<tr><th>加权<br>法</th><th>摊薄<br>法 </th></tr>');
                    }

                    if (callBackData.result.length) {
                        $ele.show();
                        for (var i = 0; i < callBackData.result.length; ++i) {
                            if (stockType == "0") {
                                arr.push('<tr><td><div class="align_right">' + stringFormatter($.trim(callBackData.result[i].ISSUED_VOLUME_A)) + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PRICE_A + '</div></td><td>' + callBackData.result[i].ISSUED_BEGIN_DATE_A + '</td>');
                                arr.push('<td><div class="align_right">' + callBackData.result[i].RAISED_MONEY_A + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PROFIT_RATE_A1 + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PROFIT_RATE_A2 + '</div></td>');
                                arr.push('<td>' + callBackData.result[i].ISSUED_MODE_CODE_A + '</td><td>' + callBackData.result[i].MAIN_UNDERWRITER_NAME_A + '</td><td><div class="align_right">' + stringFormatter($.trim(callBackData.result[i].GOT_RATE_A)) + '</div></td></tr>');
                            }
                            if (stockType != "0") {
                                arr.push('<tr><td><div class="align_right">' + stringFormatter($.trim(callBackData.result[i].ISSUED_VOLUME_A)) + '</div></td><td><div class="align_right">' + stringFormatter($.trim(callBackData.result[i].STRATEGIC_PLACEMENT_NUM)) + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PRICE_A + '</div></td><td>' + callBackData.result[i].ISSUED_BEGIN_DATE_A + '</td>');
                                arr.push('<td><div class="align_right">' + callBackData.result[i].RAISED_MONEY_A + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PROFIT_RATE_A1 + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PROFIT_RATE_A2 + '</div></td>');
                                arr.push('<td>' + callBackData.result[i].ISSUED_MODE_CODE_A + '</td><td>' + callBackData.result[i].MAIN_UNDERWRITER_NAME_A + '</td><td><div class="align_right">' + stringFormatter($.trim(callBackData.result[i].GOT_RATE_A)) + '</div></td></tr>');
                            }
                        }
                        $ele.find('table').html(arr.join(""));
                    }


                }
            }

        });
    }


    //【A股及科创板增发】
    function azf(isKcb) {
        var $ele = isKcb ? $('.sse_common_second_cn').eq(7) : $('.sse_common_second_cn').eq(1);
        $.ajax({
            url: sseQueryURL + 'commonQuery.do',
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: {
                'isPagination': false,
                'sqlId': gpzf_sid,
                'productid': codeA
            },
            success: function(callBackData) {
                //将修改成table展示
                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                    var arr = [];
                    arr.push('<tr><th rowspan="2">发行数量<br>(' + stockTypeDw + ')</th><th rowspan="2">发行<br/>价格</th><th rowspan="2">发行<br/>日期</th><th rowspan="2">配售<br>价格</th><th rowspan="2">发行方式</th><th colspan="2">发行市盈<br>率(%)</th><th rowspan="2">上市推<br>荐人</th><th rowspan="2">主承销商</th><th rowspan="2">中签<br>率%</th><th rowspan="2">老股东配<br>售比例<br>(10：?)</th></tr>');
                    arr.push('<tr><th>加<br>权<br>法</th><th>摊<br>薄<br>法 </th></tr>');
                    if (callBackData.result.length) {
                        $ele.show()
                        for (var i = 0; i < callBackData.result.length; ++i) {
                            arr.push('<tr><td><div class="align_right">' + stringFormatter(callBackData.result[i].ISSUED_VOLUME_A) + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PRICE_A + '</div></td><td>' + callBackData.result[i].ISSUED_BEGIN_DATE_A + '</td>');
                            arr.push('<td><div class="align_right">' + callBackData.result[i].ISSUED_PRICE_A + '</div></td><td><div class="line-warp">' + callBackData.result[i].ISSUED_MODE_CODE_A + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PROFIT_RATE_A1 + '</div></td>');
                            arr.push('<td><div class="align_right">' + callBackData.result[i].ISSUED_PROFIT_RATE_A2 + '</div></td><td><div class="line-warp">' + callBackData.result[i].RECOMMEND_NAME_A + '</div></td><td><div class=" line-warp">' + callBackData.result[i].MAIN_UNDERWRITER_NAME_A + '</div></td>');
                            arr.push('<td><div class="align_right">' + stringFormatter(callBackData.result[i].GOT_RATE_A) + '</div></td><td><div class="align_right">' + callBackData.result[i].SHARE_HOLDER_RATE_A + '</div></td>');
                        }
                        $ele.find('table').html(arr.join(""));
                    }
                }
            }
        });
    }


    //【A股及科创板配股】
    function apg(isKcb) {
        var $ele = isKcb ? $('.sse_common_second_cn').eq(8) : $('.sse_common_second_cn').eq(2);
        $.ajax({
            url: sseQueryURL + 'commonQuery.do',
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: {
                'isPagination': false,
                'sqlId': gppg_sid,
                'productid': codeA
            },
            success: function(callBackData) {
                //将修改成table展示
                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                    var arr = [];
                    arr.push('<tr><th>股权登记日</th><th>除权交易日</th><th>配股价格</th><th>配股比例<br/>(10：?)</th><th>配股缴款<br/>起始日</th><th>配股缴款<br/>截止日</th><th>实际配股<br/>量(' + stockTypeDw + ')</th><th>配股上市日</th></tr>');
                    if (callBackData.result.length) {
                        $ele.show();
                        for (var i = 0; i < callBackData.result.length; ++i) {
                            arr.push('<tr><td>' + callBackData.result[i].RECORD_DATE_A + '</td><td>' + callBackData.result[i].EX_RIGHTS_DATE_A + '</td><td><div class="align_right">' + callBackData.result[i].PRICE_OF_RIGHTS_ISSUE_A + '</div></td>');
                            arr.push('<td><div class="align_right">' + callBackData.result[i].RATIO_OF_RIGHTS_ISSUE_A + '</div></td><td>' + callBackData.result[i].START_DATE_OF_REMITTANCE_A + '</td><td>' + callBackData.result[i].END_DATE_OF_REMITTANCE_A + '</td>');
                            arr.push('<td><div class="align_right">' + callBackData.result[i].TRUE_COLUME_A + '</div></td><td><div class="align_right">' + callBackData.result[i].LISTING_DATE_A + '</div></td></tr>');
                        }
                        $ele.find('table').html(arr.join(""));
                    }

                }
            }
        });

    }
    //【B首次发行】
    function bsf() {
        var $ele = $('.sse_common_second_cn').eq(3);
        $.ajax({
            url: sseQueryURL + 'commonQuery.do',
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: {
                'isPagination': false,
                'sqlId': 'COMMON_SSE_ZQPZ_GPLB_CZQK_BGSCFX_S',
                'productid': codeA
            },
            success: function(callBackData) {
                //将修改成table展示
                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                    var arr = [];
                    arr.push('<tr><th rowspan="2">发行数量</th><th rowspan="2">发行价格<br/>(元人民币<br/>/美金)</th><th rowspan="2">发行时间</th><th rowspan="2">筹资总额<br>(万人民币<br/>/万美金)</th><th colspan="2">发行市盈率(%)</th><th rowspan="2">发行方式</th><th rowspan="2">主承销商</th></tr>');
                    arr.push('<tr><th>加权法</th><th>摊薄法 </th></tr>');
                    if (callBackData.result.length) {
                        $ele.show();
                        for (var i = 0; i < callBackData.result.length; ++i) {
                            arr.push('<tr><td><div class="align_right">' + stringFormatter(callBackData.result[i].ISSUED_VOLUME_B) + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PRICE_B2 + '/' + callBackData.result[i].ISSUED_PRICE_B1 + '</div></td><td>' + callBackData.result[i].ISSUED_BEGIN_DATE_B + '</td>');
                            arr.push('<td><div class="align_right">' + callBackData.result[i].RAISED_MONEY_B2 + '/' + callBackData.result[i].RAISED_MONEY_B1 + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PROFIT_RATE_B1 + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PROFIT_RATE_B2 + '</div></td>');
                            arr.push('<td>' + callBackData.result[i].ISSUED_MODE_CODE_B + '</td><td>' + callBackData.result[i].MAIN_UNDERWRITER_NAME_B + '</td></tr>');
                        }
                        $ele.find('table').html(arr.join(""));
                    }
                }
            }
        });
    }

    //【B增发】
    function bzf() {
        var $ele = $('.sse_common_second_cn').eq(4);
        $.ajax({
            url: sseQueryURL + 'commonQuery.do',
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: {
                'isPagination': false,
                'sqlId': 'COMMON_SSE_ZQPZ_GPLB_CZQK_BGZF_S',
                'productid': codeA
            },
            success: function(callBackData) {
                //将修改成table展示
                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                    var arr = [];
                    arr.push('<tr><th rowspan="2">发行数量</th><th rowspan="2">发行价格<br>(元人民币/美金)</th><th rowspan="2">发行日期</th><th rowspan="2">筹资总额<br>(万人民币/万美金)</th><th rowspan="2">发行方式</th><th colspan="2">发行市盈率(%)</th><th rowspan="2">国际协调人</th><th rowspan="2">主承销商</th></tr>');
                    arr.push('<tr><th>加权法</th><th>摊薄法 </th></tr>');
                    if (callBackData.result.length) {
                        $ele.show();
                        for (var i = 0; i < callBackData.result.length; ++i) {
                            arr.push('<tr><td><div class="align_right">' + stringFormatter(callBackData.result[i].ISSUED_VOLUME_B) + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PRICE_B2 + '/' + callBackData.result[i].ISSUED_PRICE_B1 + '</div></td><td>' + callBackData.result[i].ISSUED_BEGIN_DATE_B + '</td>');
                            arr.push('<td><div class="align_right">' + callBackData.result[i].RAISED_MONEY_B2 + '/' + callBackData.result[i].RAISED_MONEY_B1 + '</div></td><td><div class="line-warp">' + callBackData.result[i].ISSUED_PROFIT_RATE_B1 + '</div></td><td><div class="align_right">' + callBackData.result[i].ISSUED_PROFIT_RATE_B2 + '</div></td>');
                            arr.push('<td><div class="align_right">' + callBackData.result[i].ISSUED_MODE_CODE_B + '</div></td><td><div class="align_right">' + callBackData.result[i].COORDINATOR_B + '</div></td><td><div class="line-warp">' + callBackData.result[i].MAIN_UNDERWRITER_NAME_B + '</div></td></tr>');
                        }
                        $ele.find('table').html(arr.join(""));
                    }


                }
            }
        });
    }


    //【B配股】
    function bpg() {
        var $ele = $('.sse_common_second_cn').eq(5);
        $.ajax({
            url: sseQueryURL + 'commonQuery.do',
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: {
                'isPagination': false,
                'sqlId': 'COMMON_SSE_ZQPZ_GPLB_CZQK_BGPG_S',
                'productid': codeA
            },
            success: function(callBackData) {
                //将修改成table展示
                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                    var arr = [];
                    arr.push('<tr><th>股权<br/>登记日</th><th>除权<br/>基准日</th><th>最后<br/>交易日</th><th>股配股<br/>价格</th><th>美元<br/>汇率</th><th>配股<br/>比例<br/>(10：?)</th><th>配股缴款<br/>起始日</th><th>配股缴款<br/>截止日</th><th>实际<br/>配股量<br>(万股)</th><th>配股<br/>上市日</th><th>配股类<br/>别说明</th></tr>');
                    if (callBackData.result.length) {
                        $ele.show();
                        for (var i = 0; i < callBackData.result.length; ++i) {
                            arr.push('<tr><td>' + callBackData.result[i].RECORD_DATE_B + '</td><td>' + callBackData.result[i].EX_RIGHTS_DATE_B + '</td><td>' + callBackData.result[i].LAST_TRADE_DATE_B + '</td>');
                            arr.push('<td><div class="align_right">' + stringFormatter(callBackData.result[i].PRICE_OF_RIGHTS_ISSUE_B) + '</div></td><td>' + callBackData.result[i].EXCHANGE_RATE + '</td><td>' + callBackData.result[i].RATIO_OF_RIGHTS_ISSUE_B + '</td>');
                            arr.push('<td>' + callBackData.result[i].START_DATE_OF_REMITTANCE_B + '</td><td>' + callBackData.result[i].END_DATE_OF_REMITTANCE_B + '</td>');
                            arr.push('<td><div class="align_right">' + callBackData.result[i].TRUE_COLUME_B + '</div></td><td>' + callBackData.result[i].LISTING_DATE_B + '</td><td>' + callBackData.result[i].RIGHTS_TYPE + '</td></tr>');
                        }
                        $ele.find('table').html(arr.join(""));
                    }


                }
            }
        });
    }
    //特殊事件首日表现
    function tssj() {
        $.ajax({
            url: sseQueryURL + 'marketdata/tradedata/queryStockSpecialQuat.do',
            type: "post",
            dataType: "jsonp",
            jsonp: "jsonCallBack",
            jsonpCallback: "jsonpCallback" + Math.floor(Math.random() * (100000 + 1)),
            data: {
                'isPagination': true,
                'startDate': "",
                'endDate': "",
                'product': urlProductid
            },
            success: function(callBackData) {
                //将修改成table展示
                if (callBackData.result != null && callBackData.result != undefined && callBackData.result.length > 0) {
                    var arr = [];
                    arr.push('<tr><th>日期</th><th>事件</th><th>当日流通<br/>股本<br>(' + stockTypeDw + ')</th><th>开盘<br/>价(元)</th><th>最高<br/>价(元)</th><th>最低<br/>价(元)</th><th>收盘<br/>价(元)</th><th>成交量<br/>(' + stockTypeDw + ')</th><th>成交额<br/>(万元)</th><th>换手率<br/>(%)</th></tr>');
                    if (callBackData.result.length < 1) {
                        arr.push("<tr><td colspan='50'>未找到，只有0条数据！</td></tr>");
                    } else {
                        for (var i = 0; i < callBackData.result.length; ++i) {
                            arr.push('<tr><td>' + callBackData.result[i].listingDate + '</td><td>' + callBackData.result[i].listingMark + '</td><td><div class="align_right">' + callBackData.result[i].curBonus + '</div></td>');
                            arr.push('<td><div class="align_right">' + callBackData.result[i].openprice + '</div></td><td><div class="align_right">' + callBackData.result[i].highprice + '</div></td><td><div class="align_right">' + callBackData.result[i].lowprice + '</div></td>');
                            arr.push('<td><div class="align_right">' + callBackData.result[i].closeprice + '</div></td><td><div class="align_right">' + callBackData.result[i].tradingvol + '</div></td>');
                            arr.push('<td><div class="align_right">' + callBackData.result[i].tradingamt + '</div></td><td><div class="align_right">' + stringFormatter(callBackData.result[i].exchangerate + '') + '</div></td></tr>');
                        }
                        $("#tableData_stockListFirDay").find('table').html(arr.join(""));
                    }

                }
            }
        });
    }
}
