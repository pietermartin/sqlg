import m from 'mithril';
import * as ace from 'ace-builds/src-noconflict/ace';
import 'ace-builds/src-noconflict/theme-monokai';
import 'ace-builds/src-noconflict/theme-github';
import 'ace-builds/src-noconflict/mode-javascript';
import 'ace-builds/src-noconflict/mode-sql';
import 'ace-builds/src-noconflict/mode-json';

function Ace(ignore) {

    let editor;

    return {
        oncreate: function (vnode) {
            editor = ace.edit(vnode.attrs.id);
            editor.setReadOnly(vnode.attrs.readOnly !== undefined ? vnode.attrs.readOnly : true);
            ace.config.set('basePath', '');
            ace.config.set('modePath', '');
            ace.config.set('themePath', '');
            editor.setTheme("ace/theme/github");
            editor.setFontSize(14);
            switch (vnode.attrs.mode) {
                case 'sql':
                    editor.session.setMode("ace/mode/sql");
                    break;
                case 'json':
                    editor.session.setMode("ace/mode/json");
                    break;
                default:
                    editor.session.setMode("ace/mode/json");
            }
            if (vnode.attrs.text !== undefined && vnode.attrs.text !== null) {
                editor.setValue(vnode.attrs.text, -1);
            }
            if (vnode.attrs.aceEditor !== undefined) {
                vnode.attrs.aceEditor(editor);
            }
        },
        onremove: function(vnode) {
            console.log("onremove");
        },
        view: function (vnode) {
            if (vnode.attrs.refresh === true && vnode.attrs.text !== undefined && vnode.attrs.text !== null) {
                editor.setValue(vnode.attrs.text, -1);
            }
            return m("div.cm-ace", {id: vnode.attrs.id}, vnode.attrs.text);
        }
    }
}

export default Ace;
