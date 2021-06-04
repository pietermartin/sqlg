import $ from "jquery";

const Tooltip = {

    hide: function (e, tooltipId) {
        if ($(e.target).closest('.div-tooltip').length === 0) {
            $('[data-tooltip]').attr('data-tooltip-toggle', "false");
            $('.div-tooltip').hide();
            $(document).off('click', 'body', Tooltip.hide);
        }
    },

    prepare: function (options) {

        let s;
        if (options && options.selector) {
            s = options.selector;
        } else {
            s = '[data-tooltip]';
        }

        let show = function (e) {
            let element = $(this);
            let text = element.attr('data-tooltip');
            let isHtml = element.attr('data-tooltip-html');
            let isOpen = element.attr('data-tooltip-toggle');
            if (options && options.event === 'click' && isOpen && isOpen === "true") {
                element.attr('data-tooltip-toggle', "false");
                $('.div-tooltip').remove();
                $(document).off('click', 'body', Tooltip.hide);
            } else {
                element.attr('data-tooltip-toggle', "true");
                if (isHtml !== undefined) {
                    if (text && text !== '') {
                        $('.div-tooltip').remove();
                        $(document).off('click', 'body', Tooltip.hide);
                        $('<div class="div-tooltip bg-secondary"></div>').append(text).appendTo('body').fadeIn('fast');
                        if (e.type === "click") {
                            setTimeout(function () {
                                $(document).on('click', 'body', Tooltip.hide);
                            }, 100);
                        }
                    }
                } else {
                    if (text && text !== '') {
                        $('.div-tooltip').remove();
                        $(document).off('click', 'body', Tooltip.hide);
                        $('<div class="div-tooltip bg-secondary"></div>').text(text).appendTo('body').fadeIn('fast');
                    }
                }
                mousemove(e);
            }
        };
        let remove = function () {
            $('.div-tooltip').remove();
            $(document).off('click', 'body', Tooltip.hide);
        };
        let mousemove = function (e) {
            let y = $(this).attr('data-tooltip-y');
            let top = y ? e.pageY + parseInt(y) : e.pageY - 45;
            let x = $(this).attr('data-tooltip-x');
            let fromTheLeft = $(this).attr('data-tooltip-fromtheright');
            let left = x ? e.pageX + parseInt(x) : e.pageX + 10;
            let width = $(this).attr('data-tooltip-width');
            let css;
            if (width) {
                css = {top: top, left: left, "maxWidth": parseInt(width)};
            } else if (!fromTheLeft ){
                css = {top: top, left: left};
            } else {
                css = {top: top, left: left - ($(".div-tooltip").width())};
            }
            $('.div-tooltip').css(css);
        };
        if (options && options.event === 'click') {
            $(s).off('click', show);
            $(s).on('click', show);
        } else {
            $(s).hover(show, remove).mousemove(mousemove);
        }
    }
};
export default Tooltip;
