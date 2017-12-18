gopkg='github.com/Codility/redis-rpc/go/redrpc' # TODO: automatic detection
repo_root="$(git rev-parse --show-toplevel)"
gocode_root="${repo_root}/go"
export GOPATH="${gocode_root}/.gopath"
export GOBIN="${GOPATH}/bin"

gopkg_path="${GOPATH}/src/${gopkg}"
gopkg_root="${gopkg_path%/*/*}"
mkdir -p "${gopkg_root}"
ln -sf "${gocode_root}" "${gopkg_root}"

cd "${GOPATH}"
