# select build image
FROM rust:1.36 as build

# create a new empty shell project
RUN USER=root cargo new --bin warcraider
WORKDIR /warcraider

# copy over your manifests
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

# this build step will cache your dependencies
ENV RUSTFLAGS "-C target-cpu=native -C link-args=-Wl,-zstack-size=4194304"
RUN cargo build --release
RUN rm src/*.rs

# copy your source tree
COPY ./.git ./.git
COPY ./src ./src

# build for release
RUN rm ./target/release/deps/warcraider*
RUN cargo build --release

# our final base
FROM google/cloud-sdk:latest

# install dependencies
RUN apt-get install -y tidy

# copy the build artifact from the build stage
COPY --from=build /warcraider/target/release/warcraider .
ARG RUST_LOG=warcraider=info
ENV RUST_LOG $RUST_LOG

# setup google credentials in image for when not running inside google cloud
#COPY credentials.json ./
#COPY credentials.boto /root/.boto
#RUN gcloud auth activate-service-account --key-file=credentials.json
#RUN gcloud config set pass_credentials_to_gsutil false
#ENV GOOGLE_APPLICATION_CREDENTIALS credentials.json
# set the startup command to run your binary
CMD ["./warcraider"]
