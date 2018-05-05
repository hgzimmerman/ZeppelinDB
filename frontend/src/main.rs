#[macro_use]
extern crate yew;
use yew::prelude::*;

extern crate failure;

mod signup_comp;
use signup_comp::SignUpComponent;

mod transfer_comp;
use transfer_comp::TransferComponent;


type Context = ();

struct Model { }

enum Msg {
    DoIt,
    ButtonClick,
}

struct ToggleButton {
    is_on:bool,
}

impl Component<Context> for ToggleButton {

    type Msg = Msg;
    type Properties = ();

    fn create(_: Self::Properties, _: &mut Env<Context, Self>) -> Self {
        ToggleButton {
            is_on: false,
        }
    }

    fn update(&mut self, msg: Self::Msg, _: &mut Env<Context, Self>) -> ShouldRender {
        match msg {
            Msg::ButtonClick => {
                // Been clicked, toggle
                self.is_on = !self.is_on;
                true
            }
            _ => false
        }
    }
}

impl Renderable<Context, ToggleButton> for ToggleButton {
    fn view(&self) -> Html<Context, Self> {
        if self.is_on {
            html! {
                <div>
                    <button onclick=|_| Msg::ButtonClick,>{ "Button is on!" }</button>
                </div>
            }
        } else {
            html! {
                <div>
                    <button onclick=|_| Msg::ButtonClick,>{ "Button is off!" }</button>
                </div>
            }
        }

    }
}


impl Component<Context> for Model {
    // Some details omitted. Explore the examples to get more.

    type Msg = Msg;
    type Properties = ();

    fn create(_: Self::Properties, _: &mut Env<Context, Self>) -> Self {
        Model { }
    }

    fn update(&mut self, msg: Self::Msg, _: &mut Env<Context, Self>) -> ShouldRender {
        match msg {
            Msg::DoIt => {
                // Update your model on events
                true
            }
            _ => false
        }
    }
}

impl Renderable<Context, Model> for Model {
    fn view(&self) -> Html<Context, Self> {
        html! {
            // Render your model here
            <div>
                <SignUpComponent: />

                <TransferComponent: />

            </div>
        }
    }
}

fn main() {
    yew::initialize();
    let app: App<_, Model> = App::new(());
    app.mount_to_body();
    yew::run_loop();
}