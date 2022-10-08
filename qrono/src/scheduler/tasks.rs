use super::{State, Task, TaskContext};

impl<F: FnMut() -> bool + Send + 'static> Task for F {
    type Value = ();
    type Error = ();

    fn run(&mut self, _: &TaskContext<Self>) -> Result<State<Self::Value>, Self::Error> {
        Ok(if self() { State::Runnable } else { State::Idle })
    }
}

pub struct BoxFn(Box<dyn FnMut() -> bool + Send + 'static>);

impl BoxFn {
    pub fn new<F: FnMut() -> bool + Send + 'static>(f: F) -> Self {
        BoxFn(Box::new(f))
    }
}

impl Task for BoxFn {
    type Value = ();
    type Error = ();

    fn run(&mut self, _: &TaskContext<Self>) -> Result<State<Self::Value>, Self::Error> {
        Ok(if (self.0)() {
            State::Runnable
        } else {
            State::Idle
        })
    }
}
