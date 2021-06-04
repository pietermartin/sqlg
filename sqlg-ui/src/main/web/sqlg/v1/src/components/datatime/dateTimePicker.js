import m from "mithril";
import $ from "jquery";
import '@progress/kendo-ui/js/kendo.datetimepicker';


function DateTimePicker(ignore) {

    let dateTime;
    let datetimepicker;

    let InternalDateTime = function(initialDateTime) {
        this.dateTime = initialDateTime;
        this.format = function() {
            return this.dateTime.format();
        }
    };

    return {

        oninit: function(vnode) {
            dateTime = new InternalDateTime(vnode.attrs.dateTime);
        },

        oncreate: function (vnode) {
            $("#" + vnode.attrs.dateTimePickerId).kendoDateTimePicker({
                value: dateTime.format(),
                dateInput: true,
                format: "MMM dd, yyyy HH:mm",
                change: function (element) {
                    vnode.attrs.onChange(this.value());
                }
            });
            datetimepicker = $("#" + vnode.attrs.dateTimePickerId).data("kendoDateTimePicker");
        },

        view: function (vnode) {
            if (datetimepicker && vnode.attrs.enabled !== undefined) {
                datetimepicker.enable(vnode.attrs.enabled);
            }
            if (vnode.attrs.dateTime.isSame(dateTime.dateTime)) {
                return m("input", {id: vnode.attrs.dateTimePickerId, class: "kendo-date-time-picker"});
            } else {
                dateTime.dateTime = vnode.attrs.dateTime;
                datetimepicker.value(dateTime.dateTime.format());
                return m("input", {id: vnode.attrs.dateTimePickerId, class: "kendo-date-time-picker"});
            }
        }
    }
}

export default DateTimePicker
