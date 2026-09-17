# 파일 락(flock/fcntl) 지원 설계 리포트

이 문서는 구현 계획이 아니라 **어떤 방향으로, 어떤 순서로 작업할지에 대한 설계 리포트**다. 코드 변경은 포함하지 않는다.

## 1. 문제 정의

`~/Projects/irodsfs/bin/mount/gocommands`에서 `make`(= `go build`)를 실행하면
`go: RLock .../go.mod: operation not supported` 에러가 난다. `go build`는 모듈 캐시
동기화를 위해 `go.mod`에 blocking exclusive `flock()`을 거는데, FUSE 마운트가 이
요청을 지원하지 않아서 실패한다.

## 2. 현재 상태 (조사 결과)

FUSE 락은 "구현이 아예 없는" 상태가 아니라 **부분적으로 이미 구현되어 있고, 정확히
그 빠진 부분이 이번 버그**다.

- FUSE 라이브러리는 `github.com/hanwen/go-fuse/v2`. 이 라이브러리는 BSD `flock()`과
  POSIX `fcntl()`을 커널/FUSE 프로토콜 레벨에서 이미 통합해서, `Getlk` / `Setlk` /
  `Setlkw` 세 메서드로만 전달한다 (`flags`의 `LK_FLOCK` 비트로 flock 여부만 구분).
  즉 FUSE 계층에서 flock과 fcntl을 별도 오퍼레이션으로 나눠 구현할 필요는 없다.
- `irodsfs/irodsfs/filehandle.go`에 이미 `GetLocalLock` / `SetLocalLock`이 있고,
  `FileHandleLocalLockManager`(같은 디렉터리)라는 **자체 구현 in-memory 락
  매니저**가 non-blocking 요청은 정상적으로 처리한다.
- 문제는 `SetLocalLockW`(blocking 버전)가 **조건 없이 `syscall.ENOTSUP`을 반환하는
  하드코딩된 스텁**이라는 점이다. `go build`의 blocking `flock()`이 정확히 이
  경로를 타서 실패한다.
- 이 락 매니저는 **irodsfs 클라이언트 프로세스 안에만 존재**한다. iRODS/pool
  서버 쪽 인터페이스(`irodsfs-common`의 `IRODSFSClient`/`IRODSFSFileHandle`,
  `irodsfs-pool`의 proto·`PoolFileHandle`)에는 락 관련 메서드가 전혀 없다. 따라서
  지금은 **같은 파일을 서로 다른 마운트 프로세스(또는 pool 세션)로 열면 두 락
  매니저가 서로를 전혀 모른 채 둘 다 "락 획득 성공"을 돌려준다** — blocking
  ENOTSUP과는 별개로 존재하는 진짜 정확성 문제다.

## 3. 확정된 스코프

- pool server는 단일 인스턴스로 운영된다 (사용자 확인). 따라서 "여러 pool
  서버 인스턴스 간 락 조율"은 스코프 밖.
- 다른 client가 pool server를 거치지 않고 iRODS에 직접 접근해 락을 무시하는
  경우는 user 책임으로 스코프 밖.
- flock(whole-file)과 fcntl byte-range 락을 **모두** 지원한다.

## 4. 설계 방향 — 락의 진실의 원천을 클라이언트에서 pool 서버로 옮긴다

원래 아이디어("파일을 stagingfs로 끌어와서 로컬에서 락을 구현")의 방향은 맞지만,
"로컬 staged 파일에 커널 native flock()/fcntl()을 그대로 건다"는 실행 방식은
한 가지 함정 때문에 그대로 쓰기 어렵다:

> **POSIX fcntl 락은 프로세스 단위다.** pool 서버는 프로세스가 하나뿐인데 그
> 안에서 서로 다른 세션(서로 다른 마운트/사용자)이 같은 staged 파일을 다루게
> 된다. 커널 fcntl은 "같은 프로세스가 그 파일에 연 fd"를 구분하지 않으므로,
> 세션 A가 건 락을 세션 B가 (다른 fd로) 다시 요청해도 커널은 "이미 내
> 프로세스가 가진 락"으로 취급해 충돌을 감지하지 못하고, 한 세션이 파일을
> close하면 같은 프로세스가 그 파일에 대해 가진 fcntl 락이 전부(다른 세션이
> 건 것까지) 풀려버린다. flock()은 fd(open file description) 단위라 이 문제가
> 덜하지만, fcntl과 API를 통일해서 다루려면 결국 이 함정을 피해가야 한다.

**권장 방향**: pool 서버가 커널 syscall에 직접 의존하지 않고, `irodsfs`
클라이언트에 이미 있는 `FileHandleLocalLockManager`와 같은 성격의 **자체 in-memory
락 매니저를 pool 서버로 중앙화**한다. 즉:

- staging은 여전히 유효하다 — 파일 하나가 pool 서버 안에서 하나의 로컬 실체(하나의
  논리 경로/핸들)로 정규화되므로, 락 매니저가 무엇을 기준으로 충돌을 계산할지
  (세션/핸들/오프셋 범위)가 명확해진다.
- 그러나 실제 lock/unlock 판정은 커널에 위임하지 않고, pool 서버 프로세스 안의
  자료구조(세션·핸들·바이트 범위별 락 테이블)로 직접 구현한다. flock이든 fcntl
  byte-range든 이 매니저 하나로 처리 가능하다 (flock은 range가 전체 파일인 특수
  케이스로 취급).
- 이렇게 하면 같은 pool 서버를 거치는 모든 클라이언트/세션 사이에서 정확한
  상호 배제가 보장되고, "단일 pool 서버" 전제와도 정확히 맞아떨어진다.

## 5. 저장소별 변경 범위 (설계 수준, 코드 없음)

### irodsfs-pool (이 저장소)
- `service/api/pool.proto`에 `Getlk` / `Setlk` / `Setlkw` RPC 추가. 요청 필드는
  대략 `SessionId`, `FileHandleId`, lock owner(uint64), `FileLock{Start, End,
  Type, Pid}`, flock 여부 플래그.
- 세션/핸들 스코프의 자체 락 매니저 신규 구현 (irodsfs의
  `FileHandleLocalLockManager`를 참고해 pool 서버용으로 이식/중앙화).
- 블로킹 `Setlkw`는 실제로 요청을 대기시켜야 하므로, gRPC 컨텍스트 취소를
  구독해서 클라이언트가 연결을 끊거나 요청을 취소하면 대기를 즉시 풀어줘야 한다.
- 파일이 아직 staging되지 않은 상태(캐시 전용 읽기 등)에서 락 요청이 오면 어떻게
  할지 정책 결정 필요 (강제 staging 트리거 vs 명시적 실패).
- 세션 복구/크래시 정리 경로(`service/session_recovery.go`)에 "세션이 죽으면 그
  세션이 보유한 락을 전부 해제" 훅 추가.

### irodsfs-common (공유 인터페이스)
- `irods/interface.go`의 `IRODSFSClient`/`IRODSFSFileHandle`에 `Getlk`/`Setlk`/
  `Setlkw` 메서드 추가.
- pool을 거치지 않는 direct iRODS 접근 드라이버는 이 메서드들을 어떻게 처리할지
  결정 필요 (기존처럼 클라이언트 로컬 락으로 폴백할지, 명시적 미지원으로 둘지).
- 현재 로컬 checkout이 origin보다 한 커밋 뒤처져 있음 — 작업 시작 전 `git pull`
  필요.

### irodsfs (FUSE 클라이언트)
- `filehandle.go`의 `Getlk`/`Setlk`/`SetLocalLockW`가 pool 백엔드로 동작 중일
  때는 로컬 `FileHandleLocalLockManager` 대신 새 `IRODSFSFileHandle.Getlk/Setlk/
  Setlkw`로 위임하도록 변경.
- 기존 `FileHandleLocalLockManager`는 pool을 쓰지 않는 direct 모드를 위해 남겨둘지,
  완전히 제거하고 모든 경로를 pool로 통일할지 결정 필요.
- go-fuse가 락 대기 중 `FUSE_INTERRUPT`(커널의 lock 취소 신호)를 컨텍스트
  취소로 넘겨주는지 확인 필요 — 안 넘겨주면 blocking 요청이 클라이언트/서버
  양쪽에서 영원히 걸릴 위험이 있다.

## 6. 열린 질문 (구현 착수 전 확정 필요)

1. 파일이 staging되지 않은 상태에서 락 요청이 오면 강제로 staging할지, 실패
   처리할지.
2. `irodsfs-common` 로컬 checkout을 pull한 뒤에도 인터페이스 설계가 달라지지
   않는지 재확인.
3. go-fuse의 인터럽트/취소 전달 여부 (blocking 락의 안전한 취소를 위해 필수).
4. `FileHandleLocalLockManager`를 pool 서버로 완전히 이전하고 클라이언트 쪽은
   제거할지, 아니면 direct 모드 하위 호환을 위해 클라이언트 로컬 구현도 유지할지.

## 7. 다음 단계

위 열린 질문에 대한 답이 정해지면, 저장소별로 구체적인 구현 플랜(파일/함수
단위)을 다시 작성한다. 지금 단계에서는 코드를 건드리지 않는다.
