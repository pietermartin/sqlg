import m from 'mithril';
import Button from "../form/button";

function TransferList(ignore) {
    let dataSource, filterReportName, selectedReport;
    let enabledArray = [];
    let enableTop = 'disabled';
    let enableBottom = 'disabled';
    let leftChecked = false;
    let rightChecked = false;
    let selectedItems = [];

    let setSelected = function(report, list){
        if(typeof report['selected'] !== 'undefined') {
            report['selected'] != report['selected'];
        }

    };

    let moveAndSave = function (disableOrEnable) {

        if (disableOrEnable) {
            enabledArray.push(selectedReport);
            dataSource.capable.forEach(function (item, i) {
                if (item.id === selectedReport) {
                    dataSource.capable.splice(i, 1);
                    dataSource.enabled.push(item);
                }
            });
        } else {
            enabledArray.forEach(function (item, i) {
                if (item === selectedReport) {
                    enabledArray.splice(i, 1);
                }
            });
            dataSource.enabled.forEach(function (item, i) {
                if (item.id === selectedReport) {
                    dataSource.enabled.splice(i, 1);
                    dataSource.capable.push(item);
                }
            });
        }
        enableTop = 'disabled';
        enableBottom = 'disabled';
        selectedReport = undefined;

    };

    return {
        oninit: function (vnode) {
            dataSource = vnode.attrs.capableEnabled;
            dataSource.enabled.forEach(function (report) {
                enabledArray.push(report.id)
            });
        },
        view: function (vnode) {
            return m("div.container", {id: vnode.attrs.id}, [
                m('div.row.mt-1', [
                    m('div.col-5', [
                        m('div.border.rounded', [
                            m('div.text-center.border-bottom.rounded.align-middle.bg-light', [
                                m("input", {
                                    class:"align-middle",
                                    'type':'checkbox',
                                    checked: leftChecked,
                                    onclick: function (e) {

                                    }
                                }),
                                m('label.text-body.mb-0.p-2', 'Availible')
                            ]),
                            m("div.mt-1", {class: "input-group"}, [
                                m("div.input-group-prepend", [
                                    m('div.input-group-text.bg-transparent', m('i.fas.fa-search'))
                                ]),
                                m("input.form-control.border-right-0[type=text][placeHolder=Report Filter]", {
                                    value: filterReportName,
                                    oninput: function (e) {
                                    }
                                }),
                            ]),
                            m('div.shuffleBox', [
                                m('ul.list-group.list-group-flush', [
                                    (dataSource.capable.length > 0) ? dataSource.capable.map(function (report) {
                                        return m('li', {
                                            class: 'py-2 list-group-item' + (selectedReport === report.id ? ' active bg-transparent text-primary border-0 mt-0' : ''),
                                            key: report.id, onclick: function (e) {
                                                setSelected(report, 'left');
                                                selectedReport = report.id;
                                                enableTop = 'disabled';
                                                enableBottom = '';
                                                e.redraw = false;
                                            }
                                        }, [
                                            m('label',[
                                                m("input", {
                                                    class:"align-middle mr-2",
                                                    'type':'checkbox',
                                                    'data-id':report.id,
                                                    checked: report['selected'] ? report['selected'] : false,
                                                }),
                                            ], `${report.name}`)
                                        ])
                                    }) : m('label.text-center.mt-1', 'No Data')
                                ])
                            ])
                        ])
                    ]),
                    m('div.col-2', [
                        m(Button, {
                            class: "btn-light btn-sm " + enableTop,
                            text: "Disable",
                            icon: "fa-chevron-left",
                            onclick: function (e) {
                                if (enableTop === '') {
                                    moveAndSave(false);
                                }
                            }
                        }),
                        m(Button, {
                            class: "btn-light btn-sm " + enableBottom,
                            text: "Enable",
                            icon: "fa-chevron-right",
                            onclick: function (e) {
                                if (enableBottom === '') {
                                    moveAndSave(true);
                                }
                            }
                        })
                    ]),
                    m('div.col-5', [
                        m('div.border.rounded', [
                            m('div.text-center.border-bottom.rounded.align-middle.bg-light', [
                                m("input", {
                                    class:"align-middle",
                                    'type':'checkbox',
                                    checked: rightChecked,
                                    onclick: function (e) {

                                    }
                                }),
                                m('label.text-body.mb-0.p-2', 'Enabled')
                            ]),
                            m("div.mt-1", {class: "input-group"}, [
                                m("div.input-group-prepend", [
                                    m('div.input-group-text.bg-transparent', m('i.fas.fa-search'))
                                ]),
                                m("input.form-control.border-right-0[type=text][placeHolder=Report Filter]", {
                                    value: filterReportName,
                                    oninput: function (e) {
                                    }
                                }),
                            ]),
                            m('div.shuffleBox', [
                                m('ul.list-group.list-group-flush', [
                                    (dataSource.enabled.length > 0) ? dataSource.enabled.map(function (report) {
                                        return m('li', {
                                            class: 'py-2 list-group-item border-0' + (selectedReport === report.id ? ' active bg-transparent text-primary border-0 mt-0' : ''),
                                            key: report.id, onclick: function (e) {
                                                selectedReport = report.id;
                                                enableTop = '';
                                                enableBottom = 'disabled';
                                                m.redraw();
                                            }
                                        }, [
                                            m('label',[
                                                m("input", {
                                                    class:"align-middle mr-2",
                                                    'type':'checkbox',
                                                    'data-id':report.id,
                                                    checked: false,
                                                }),
                                            ], `${report.name}`)
                                        ])
                                    }) : m('label.text-center.mt-1', 'No Data')
                                ])
                            ])
                        ])
                    ]),
                ])
            ]);
        }
    }
}

export default TransferList;