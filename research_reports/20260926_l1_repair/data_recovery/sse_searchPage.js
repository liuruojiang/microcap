define([], function () {
    var sseF = {};
    //初始化详情页
    sseF.initDomEvents = function () {

        /*窗口滚动时固定定位搜索框*/
        var $scrollInput = $('.sse_query_input'); 
        var $windowWidth = $(window).width();
        if ($scrollInput.length > 0 && $windowWidth > 768) {
            var $offsetTop = $scrollInput.offset().top;
            //console.log($offsetTop);
            if ($(window).scrollTop() > 300) {
                $scrollTop();
            }
            $(window).scroll(function () {
                $scrollTop();
            });
        }

        function $scrollTop() {
            var $Top = $(window).scrollTop();
            //console.log($offsetTop);
            if ($Top > $offsetTop) {
                $scrollInput.css({
                        'position': 'fixed',
                        'top': 0,
                        'left': 0,
                        'z-index': '100',
                        'box-shadow': '0 3px 7px rgba(0,0,0,0.13)'
                    })
                    .parent().css({
                        'padding-top': '90px'
                    }).find('.query_hot').css('font-size', '14px');
            } else {
                $scrollInput.css({
                        'position': 'static',
                        'box-shadow': 'none'
                    })
                    .parent().css({
                        'padding-top': 10
                    }).find('.query_hot').css('font-size', '16px');
            }
        }



        /*检索页手机版标题/全文切换*/
        var $btn_change = $('.mobile_btn');
        $btn_change.on('click', 'button', function (e) {
            e.preventDefault();
            var $this = $(this);
            $this.addClass('active')
                .siblings().removeClass('active');
        });
        /*- end -*/
        
        /*调整选择全文|标题的下拉框 */
        var $selBox = $('.selBox');
        if( $selBox.length > 0 ){
            require(['selectjs'],function(){
                //alert($('.selBox').length);
            });
        }
        /*- end -*/
    };
    sseF.init = function () {
        var self = this;
        self.initDomEvents();
    };

    return sseF;

});