// CITA
// Copyright 2016-2017 Cryptape Technologies LLC.

// This program is free software: you can redistribute it
// and/or modify it under the terms of the GNU General Public
// License as published by the Free Software Foundation,
// either version 3 of the License, or (at your option) any
// later version.

// This program is distributed in the hope that it will be
// useful, but WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
// PURPOSE. See the GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

use super::{AuthorityRound, Engine};
use engine_json::{Engine as EngineJson, Spec as SpecJson};
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::sync::Arc;
use std::sync::mpsc::{channel, Receiver, Sender};

pub struct Spec {
    pub name: String,
    pub engine: Arc<Engine>,
    pub rx: Receiver<usize>,
}

impl From<SpecJson> for Spec {
    fn from(s: SpecJson) -> Self {
        let (tx, rx) = channel();
        Spec {
            name: s.name.clone().into(),
            engine: Spec::engine(s.engine, tx),
            rx: rx,
        }
    }
}

impl Spec {
    fn engine(engine_json: EngineJson, tx: Sender<usize>) -> Arc<Engine> {
        match engine_json {
            EngineJson::AuthorityRound(authority_round) => {
                AuthorityRound::new(From::from(authority_round.params), tx)
                    .expect("Failed to start AuthorityRound consensus engine.")
            }
            _ => {
                panic!("Failed to start AuthorityRound consensus engine.");
            }
        }
    }

    pub fn load<R>(reader: R) -> Result<Self, String>
    where
        R: Read,
    {
        match SpecJson::load(reader) {
            Ok(spec) => Ok(spec.into()),
            _ => Err("Spec json is invalid".into()),
        }
    }

    pub fn new_test_round(path: &str) -> Self {
        let config_file = File::open(path).unwrap();
        let fconfig = BufReader::new(config_file);
        Spec::load(fconfig).expect(concat!("spec is invalid."))
    }
}
