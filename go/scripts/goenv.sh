gopkg='github.com/Codility/redis-rpc/go/redrpc' # TODO: automatic detection
repo_root="$(git rev-parse --show-toplevel)"
gocode_root="${repo_root}/go"
export GOPATH="${gocode_root}/.gopath"
export GOBIN="${GOPATH}/bin"

gopkg_path="${GOPATH}/src/${gopkg}"
gopkg_root="${gopkg_path%/*/*}"
echo mkdir -p "${gopkg_root}"
mkdir -p "${gopkg_root}"
echo ln -sf "${gocode_root}" "${gopkg_root}/go"
ln -sf "${gocode_root}" "${gopkg_root}"

cd "${GOPATH}"
echo PWD `pwd`