# Homebrew formula for loco — the Local Operator. Rendered by the release flow; do not hand-edit.
#
# Placeholders are substituted by .github/workflows/release-please.yml:
#   {{VERSION}}            — bare semver, e.g. 0.2.0 (no leading "v")
#   {{TAG}}                — tap release tag, e.g. loco-v0.2.0
#   {{SHA256_DARWIN_ARM64}} etc. — per-artifact checksums from goreleaser's checksums.txt
#
# Assets are hosted on the PUBLIC amitray007/homebrew-tap release, not on the
# loco repo's own release: loco is private, so an unauthenticated `brew install`
# cannot fetch from it (verified: those URLs 404 while the tap's return 200).
# If loco ever goes public, the urls below can point at the source repo release
# directly and the tap release becomes redundant.
class Loco < Formula
  desc "Local Operator: a durable local runner (cron + services) and event bus"
  homepage "https://github.com/amitray007/loco"
  version "{{VERSION}}"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/amitray007/homebrew-tap/releases/download/{{TAG}}/loco_{{VERSION}}_darwin_arm64.tar.gz"
      sha256 "{{SHA256_DARWIN_ARM64}}"
    end
    on_intel do
      url "https://github.com/amitray007/homebrew-tap/releases/download/{{TAG}}/loco_{{VERSION}}_darwin_amd64.tar.gz"
      sha256 "{{SHA256_DARWIN_AMD64}}"
    end
  end
  on_linux do
    on_arm do
      url "https://github.com/amitray007/homebrew-tap/releases/download/{{TAG}}/loco_{{VERSION}}_linux_arm64.tar.gz"
      sha256 "{{SHA256_LINUX_ARM64}}"
    end
    on_intel do
      url "https://github.com/amitray007/homebrew-tap/releases/download/{{TAG}}/loco_{{VERSION}}_linux_amd64.tar.gz"
      sha256 "{{SHA256_LINUX_AMD64}}"
    end
  end

  def install
    bin.install "loco"
  end

  def caveats
    <<~EOS
      loco is installed, but the background daemon is NOT started automatically.
      To install and start it (launchd on macOS, systemd user units on Linux):
        loco daemon install
      The daemon needs an active login session. Check health with: loco doctor
    EOS
  end

  test do
    assert_match "loco", shell_output("#{bin}/loco version")
  end
end
