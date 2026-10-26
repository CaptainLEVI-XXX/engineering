# Read before getting started!!

First, read [src/main.rs](https://github.com/voluntas-io/engineering-challenge/blob/main/src/main.rs). Then, some background on this repo:

This repo uses the [FIX SBE standard](https://fixtrading.org/packages/simple-binary-encoding-technical-specification-v2-rc2/)
for writing binary messages. The rust code abstracts this away for you, and you can simply use the helpers, as demonstrated in `main.rs`.

### More background, in case you are curious!

FIX SBE is defined in XML, which I find extremely cumbersome to edit, especially
when updating schemas frequently. To solve this, I have also introduced some
helper code that takes our own custom YAML format, and turns it into FIX SBE
XML, which in turn is used to generate rust code to encode and decode the
messages. **You don't need to edit any of these files**. This explanation is
simply to help you understand the origins of the various files in this repo.

If you're curious to see the YAML schema, take a look at [challenge_orderbook.yaml](https://github.com/voluntas-io/engineering-challenge/blob/main/sbe_schemas/engineering_challenge/challenge_orderbook.yaml).
We also use a custom timestamp field, so that the Rust struct can use
`chrono::DateTime` in the generated struct, and in SBE schema, it is a uint64
representing the timestamp as the number of microseconds since unix epoch.



you are staff system engineer specializes in building low latency and acuurate system and have indepth understabding od rust its compiler and bytes level optmization without loosing any data(accurancy is important) ...what i want you to do write an AGENT.md file specializes in system engineering , building High freequcny based system and extracting the langauage core for follwoing the best engineering practices......so do a proper research on how we build HFT based system in rust and we are trying to https://voluntas.io/engineering-challenge do this ....but before this I want you write an proper Agent.md file that can understand about the first the problem we are tryting to solve and give us all the langauage and design features to solve that ......we will be using polymarket instead of kalshi ....did you get the requirement ....before writing the AGENT.md ...tll how will do you research first