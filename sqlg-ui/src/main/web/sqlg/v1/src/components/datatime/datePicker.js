import m from "mithril";
import $ from "jquery";
import '@progress/kendo-ui/js/kendo.datepicker';


function DatePicker(ignore) {

    let date;
    let datepicker;

    let InternalDate = function(initialDate) {
        this.date = initialDate;
        this.format = function() {
            return this.date.format("MMM D, YYYY");
        }
    };

    return {
        oninit: function(vnode) {
            if (!!vnode.attrs.date) {
                date = new InternalDate(vnode.attrs.date);
            }
        },

        oncreate: function (vnode) {
            $("#" + vnode.attrs.datePickerId).kendoDatePicker({
                value: !!date ? date.format() : undefined,
                dateInput: true,
                format: "MMM dd, yyyy",
                change: function (element) {
                    vnode.attrs.onChange(this.value());
                }
            });
            datepicker = $("#" + vnode.attrs.datePickerId).data("kendoDatePicker");
        },

        view: function (vnode) {
            if (datepicker && vnode.attrs.enabled !== undefined) {
                datepicker.enable(vnode.attrs.enabled);
            }

            if (!date && !vnode.attrs.date) {
                return m("input", {id: vnode.attrs.datePickerId, class: "kendo-date-picker"});
            }
            if (!!date && !vnode.attrs.date) {
                return m("input", {id: vnode.attrs.datePickerId, class: "kendo-date-picker"});
            }
            if (!date && !!vnode.attrs.date) {
                date = new InternalDate(vnode.attrs.date);
                return m("input", {id: vnode.attrs.datePickerId, class: "kendo-date-picker"});
            }

            if (vnode.attrs.date.isSame(date.date)) {
                return m("input", {id: vnode.attrs.datePickerId, class: "kendo-date-picker"});
            } else {
                date.date = vnode.attrs.date;
                datepicker.value(date.date.format("MMM D, YYYY"));
                return m("input", {id: vnode.attrs.datePickerId, class: "kendo-date-picker"});
            }
        }

    }
}

export default DatePicker
