import $ from "jquery";

import "../../assets/slickgrid/lib/jquery-1.12.4.min";
import "../../assets/slickgrid/lib/jquery.event.drag-2.3.0";
import "../../assets/slickgrid/lib/jquery-ui-1.11.3.min";
import "../../assets/slickgrid/css/smoothness/jquery-ui.css";
import "../../assets/slickgrid/slick.core";
import "../../assets/slickgrid/slick.grid";
// import "../../assets/slickgrid/plugins/slick.cellselectionmodel";
import "../../assets/slickgrid/plugins/slick.rowselectionmodel";
// import "../../assets/slickgrid/plugins/slick.cellrangeselector";
// import "../../assets/slickgrid/plugins/slick.cellrangedecorator";
import "../../assets/slickgrid/slick.grid.css";

import "./slick.tree.dataview";

import "./slicktree.scss"

class SlickTree {
    constructor({root, data}) {
        this.root = $('#' + root)
        this.root.append(`<div></div></div><div id="${root + '_slickgrid'}" class="slick-tree"></div>`);
        this.gridDiv = document.querySelector('#' + root + "_slickgrid");
        let columns = [
            {id: "title", name: "Title", field: "title", width: 220}
        ];
        let options = {
            enableCellNavigation: true,
            showHeaderRow: false,
            headerRowHeight: 35,
            editable: false,
            enableAddRow: false,
            asyncEditorLoading: false,
            frozenColumn: 1

        };
        this.dataView = new Slick.Data.DataTreeView({treeView: true});
        this.dataView.setFilter(this.filter);
        this.dataView.setItems(data);
        this.grid = new Slick.Grid(this.gridDiv, this.dataView, columns, options);
        // this.grid.setSelectionModel(new Slick.CellSelectionModel({selectActiveRow: true}));
        this.grid.setSelectionModel(new Slick.RowSelectionModel());
        //remove the column's header
        $('#' + this.gridDiv.id + " .slick-header-columns").css("height", "0px");
        this.grid.resizeCanvas();
    }

    filter(item, args) {
        return true;
    };

}

export default SlickTree
