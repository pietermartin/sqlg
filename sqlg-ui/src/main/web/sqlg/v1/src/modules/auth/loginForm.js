import m from 'mithril';
import Button from "../../components/form/button";
import ToastContainer from "../../components/toast/toastContainer";

function LoginForm(ignore) {

    return {
        oncreate: function ({attrs: {state, actions}}) {
            $("#usernameInput").focus();
            setTimeout(() => {
                m.redraw();
            }, 0);
        },
        view: ({attrs: {state, actions}}) => {
            return m("div.container.mt-5",
                m("form", [
                    m("legend.mb-3", "Sign in to Sqlg's ui"),
                    m("div.mb-3", [
                        m("label.form-label", {for: "usernameInput"}, "username"),
                        m("input#usernameInput.form-control", {type: "username", oninput: actions.loggedInUsername})

                    ]),
                    m("div.mb-3", [
                        m("label.form-label", {for: "passwordInput"}, "password"),
                        m("input#passwordInput.form-control", {type: "password", oninput: actions.loggedInPassword})
                    ]),
                    m(Button, {
                        type: "submit",
                        class: "btn-primary",
                        icon: "fas fa-sign-in-alt",
                        text: "Submit",
                        onclick: (e) => {
                            if (state.loggedInUsername !== undefined && state.loggedInUsername !== "") {
                                actions.login(e);
                            }
                            e.redraw = false;
                            e.preventDefault();
                        }
                    })
                ])
            );
        }
    }
}

export default LoginForm;
