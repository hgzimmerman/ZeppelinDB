use yew::prelude::*;


type Context = ();

pub struct SignUpComponent {
    username: String,
    password: String,
}

#[derive(Debug)]
pub enum Msg {
    UserUpdate(String),
    PasswordUpdate(String),
    Submit,
    NoOp,
}

impl Component<Context> for SignUpComponent {

    type Msg = Msg;
    type Properties = ();

    fn create(_: Self::Properties, _: &mut Env<Context, Self>) -> Self {
        SignUpComponent {
            username : "".to_string(),
            password : "".to_string(),
        }
    }

    fn update(&mut self, msg: Self::Msg, _: &mut Env<Context, Self>) -> ShouldRender {
        match msg {
            Msg::UserUpdate(text) => {
                self.username = text;
                true
            }
            Msg::PasswordUpdate(text) =>{
                self.password = text;
                true
            }
            Msg::Submit => {
                self.username = "".to_string();
                self.password = "".to_string();
                println!("Submit pressed");
                true
            }
            _ => {
                false
            }

         }
    }
}

impl Renderable<Context, SignUpComponent> for SignUpComponent {
    fn view(&self) -> Html<Context, Self> {
        html! {
            <div>
                <b>{ "Create a new user: "}</b>
                { "Username: " }
                <input
                    placeholder="Username",
                    value=&self.username,
                    oninput=|e: InputData| Msg::UserUpdate(e.value),
                    onkeypress=|e: KeyData| {
                        if e.key == "Enter" { Msg::Submit } else {Msg::NoOp}
                    },
                />
                { "Password: " }
                <input
                    placeholder="Password",
                    value=&self.password,
                    oninput=|e: InputData| Msg::PasswordUpdate(e.value),
                    onkeypress=|e: KeyData| {
                        if e.key == "Enter" { Msg::Submit } else {Msg::NoOp}
                    },
                />

                <button onclick=|_| Msg::Submit,>{ "Submit" }</button>
            </div>
        }
    }
}
