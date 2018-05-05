use yew::prelude::*;


type Context = ();

pub struct TransferComponent {
    src_user: String,
    dest_user: String,
    amount: u32,
    src_password: String,
}

#[derive(Debug)]
pub enum Msg {
    SourceUserUpdate(String),
    SourcePasswordUpdate(String),
    DestUserUpdate(String),
    AmountUpdate(String),
    Submit,
    NoOp,
}

impl Component<Context> for TransferComponent {

    type Msg = Msg;
    type Properties = ();

    fn create(_: Self::Properties, _: &mut Env<Context, Self>) -> Self {
        TransferComponent {
            src_user: "".to_string(),
            dest_user: "".to_string(),
            amount: 0,
            src_password: "".to_string(),
        }
    }

    fn update(&mut self, msg: Self::Msg, _: &mut Env<Context, Self>) -> ShouldRender {
        match msg {
            Msg::SourceUserUpdate(text) => {
                self.src_user = text;
                true
            }
            Msg::SourcePasswordUpdate(text) =>{
                self.src_password = text;
                true
            }
            Msg::DestUserUpdate(text) => {
                self.dest_user = text;
                true
            }
            Msg::AmountUpdate(text) => {
                self.amount = text.parse::<u32>().unwrap_or(0);
                true
            }
            Msg::Submit => {
                self.src_user = "".to_string();
                self.src_password = "".to_string();
                self.dest_user = "".to_string();
                self.amount = 0;
                println!("Submit pressed");
                true
            }
            _ => {
                false
            }

         }
    }
}

impl Renderable<Context, TransferComponent> for TransferComponent {
    fn view(&self) -> Html<Context, Self> {
        html! {
            <div>
                <b>{ "Send money to another account: "}</b>
                { "Source user: " }
                <input
                    placeholder="Source",
                    value=&self.src_user,
                    oninput=|e: InputData| Msg::SourceUserUpdate(e.value),
                    onkeypress=|e: KeyData| {
                        if e.key == "Enter" { Msg::Submit } else {Msg::NoOp}
                    },
                />
                { "sending Amount: " }
                <input
                    placeholder="0",
                    value=&self.amount.to_string(),
                    oninput=|e: InputData| Msg::AmountUpdate(e.value),
                    onkeypress=|e: KeyData| {
                        if e.key == "Enter" { Msg::Submit } else {Msg::NoOp}
                    },
                />
                { "to Destination User: " }
                <input
                    placeholder="Destination",
                    value=&self.dest_user,
                    oninput=|e: InputData| Msg::DestUserUpdate(e.value),
                    onkeypress=|e: KeyData| {
                        if e.key == "Enter" { Msg::Submit } else {Msg::NoOp}
                    },
                />
                { "Confirm source user's password: " }
                <input
                    placeholder="Src Password",
                    value=&self.src_password,
                    oninput=|e: InputData| Msg::SourcePasswordUpdate(e.value),
                    onkeypress=|e: KeyData| {
                        if e.key == "Enter" { Msg::Submit } else {Msg::NoOp}
                    },
                />

                <button onclick=|_| Msg::Submit,>{ "Submit" }</button>
            </div>
        }
    }
}
