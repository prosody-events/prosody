# Changelog

## [0.2.0](https://github.com/prosody-events/prosody/compare/prosody-v0.1.2...prosody-v0.2.0) (2026-04-28)


### Features

* **codec:** support custom wire formats via Codec trait ([#26](https://github.com/prosody-events/prosody/issues/26)) ([f98eeb2](https://github.com/prosody-events/prosody/commit/f98eeb2a716f7600187bdc1a6b948d79d31add76))
* **middleware:** apply hooks (after_commit / after_abort) on FallibleHandler ([#24](https://github.com/prosody-events/prosody/issues/24)) ([4b0f2de](https://github.com/prosody-events/prosody/commit/4b0f2de6fa21f30e2c4a64ec6ce4262ad7a4f0ad))


### Bug Fixes

* use client-side monotonic timestamps for Cassandra writes ([#25](https://github.com/prosody-events/prosody/issues/25)) ([f4a4ba2](https://github.com/prosody-events/prosody/commit/f4a4ba2fb60c285e3031562c464b3c1f543df62c))


### Performance Improvements

* **telemetry:** increase broadcast channel capacity to 32768 ([#19](https://github.com/prosody-events/prosody/issues/19)) ([9850295](https://github.com/prosody-events/prosody/commit/9850295d935086e134980f52fb3e44c624f1b537))

## [0.1.2](https://github.com/prosody-events/prosody/compare/prosody-v0.1.1...prosody-v0.1.2) (2026-04-20)


### Bug Fixes

* **consumer:** use MemoryLoader for defer middleware in mock mode ([#16](https://github.com/prosody-events/prosody/issues/16)) ([295dc0c](https://github.com/prosody-events/prosody/commit/295dc0c77af102ba471ebb596855d71ce52df2fd))


### Performance Improvements

* **defer:** eliminate get_next tombstone scans with static hints ([#18](https://github.com/prosody-events/prosody/issues/18)) ([f1a9a9b](https://github.com/prosody-events/prosody/commit/f1a9a9be46c21a5f76964969b978900a06fdd0d0))

## [0.1.1](https://github.com/prosody-events/prosody/compare/prosody-v0.1.0...prosody-v0.1.1) (2026-04-10)


### Performance Improvements

* eliminate slab scans from clear_and_schedule ([#13](https://github.com/prosody-events/prosody/issues/13)) ([23e7c26](https://github.com/prosody-events/prosody/commit/23e7c26af2453dbadaa4ed9ea92371294bd23936))

## 0.1.0 (2026-04-09)


### Features

* add deduplication cache to producer ([#67](https://github.com/prosody-events/prosody/issues/67)) ([dbc4ca8](https://github.com/prosody-events/prosody/commit/dbc4ca86ee78ebfcf570a5c5080ff64b186a8a49))
* add prelude module and promote re-exports ([#8](https://github.com/prosody-events/prosody/issues/8)) ([c28769e](https://github.com/prosody-events/prosody/commit/c28769ea22b2331e01333599ba168004fae848b9))
* add QoS middleware for fair scheduling, deferred retry, and monopolization prevention ([#129](https://github.com/prosody-events/prosody/issues/129)) ([4be7757](https://github.com/prosody-events/prosody/commit/4be77572ad2e3105c8badf82b6b87257d88cbb95))
* add source system deduplication and upgrade dependencies ([#72](https://github.com/prosody-events/prosody/issues/72)) ([c9b987b](https://github.com/prosody-events/prosody/commit/c9b987b800908c1741e3c247a2a80d561fbbe701))
* add SystemTime conversions for CompactDateTime ([#138](https://github.com/prosody-events/prosody/issues/138)) ([07748b6](https://github.com/prosody-events/prosody/commit/07748b69fb8e2bc86f953823ad9469968f4887f9))
* add timer defer middleware for non-blocking key retry ([#134](https://github.com/prosody-events/prosody/issues/134)) ([438fedb](https://github.com/prosody-events/prosody/commit/438fedb5fa8e58f4f754f09c30dd43514723e0c8))
* **admin:** support delete topics ([#42](https://github.com/prosody-events/prosody/issues/42)) ([c5c7284](https://github.com/prosody-events/prosody/commit/c5c728412829eefed2978952a4ec67673c3f9e89))
* allow tasks to listen for partition shutdown ([6647f4e](https://github.com/prosody-events/prosody/commit/6647f4e3ee1110736cde825d0131183a1a7d4a6c))
* basic consumer messages and offset tracking ([9470eba](https://github.com/prosody-events/prosody/commit/9470ebacd5d6083d2f7a2ef8240dfe020bfc113a))
* basic poll loop ([7eed788](https://github.com/prosody-events/prosody/commit/7eed788d5e074943f696fdf8d2842199a2cedbb7))
* best-effort mode ([#62](https://github.com/prosody-events/prosody/issues/62)) ([77b55db](https://github.com/prosody-events/prosody/commit/77b55db4658d20a61f6a88d04fd05f65fa7a9d19))
* **cache:** replace LRU with Quick Cache and fix idempotence race condition ([#93](https://github.com/prosody-events/prosody/issues/93)) ([37d8c6a](https://github.com/prosody-events/prosody/commit/37d8c6af6765f42fc6124917adc9bde6086b1f34))
* CI ([#1](https://github.com/prosody-events/prosody/issues/1)) ([194dca6](https://github.com/prosody-events/prosody/commit/194dca609ef331615bf0a63e4289e6305790396d))
* **combined:** don’t require exclusive access for subscription and unsubscription ([#33](https://github.com/prosody-events/prosody/issues/33)) ([f207f65](https://github.com/prosody-events/prosody/commit/f207f65c481d95d5b79bdabc959c2f94ff3b7ce4))
* **combined:** Implement CombinedClient for unified producer and consumer operations ([#31](https://github.com/prosody-events/prosody/issues/31)) ([c9052cc](https://github.com/prosody-events/prosody/commit/c9052cc5304bd066dfcb99752be1581b8e6a11ba))
* commit offsets ([211dac6](https://github.com/prosody-events/prosody/commit/211dac67c69fcaa9e803be7852f1be25b0e748e7))
* configurable OTel span relation for message and timer execution ([#161](https://github.com/prosody-events/prosody/issues/161)) ([e528e7c](https://github.com/prosody-events/prosody/commit/e528e7cb95cb1fe57b5601dd291b88b41dcfd1b8))
* consumer interface and config ([2039eeb](https://github.com/prosody-events/prosody/commit/2039eebfdf2c943bb23c2a09d73444aebcf21fc6))
* **consumer:** add event type filtering ([#73](https://github.com/prosody-events/prosody/issues/73)) ([0d7c964](https://github.com/prosody-events/prosody/commit/0d7c964222cf4baa2408d708dc111cc81dfe67ce))
* **consumer:** add health check probes and stall detection ([#55](https://github.com/prosody-events/prosody/issues/55)) ([e975c86](https://github.com/prosody-events/prosody/commit/e975c86a5ec3f3ab31ceddd941683d1afe8b7712))
* **consumer:** add poll and partition event loops to liveness checks ([#88](https://github.com/prosody-events/prosody/issues/88)) ([79da6bf](https://github.com/prosody-events/prosody/commit/79da6bf9b42ce00577fc5c4f800bb0c80d18bfae))
* **consumer:** add shutdown timeout parameter ([#78](https://github.com/prosody-events/prosody/issues/78)) ([901a1ab](https://github.com/prosody-events/prosody/commit/901a1abfbfd690e2b4cda4b6888c63994d281a3c))
* **consumer:** Add timer scheduling system ([#97](https://github.com/prosody-events/prosody/issues/97)) ([742d53e](https://github.com/prosody-events/prosody/commit/742d53e0fdcf551c0ce4b4a7315de01dd5e8172f))
* **consumer:** Improve error handling and shutdown process ([#26](https://github.com/prosody-events/prosody/issues/26)) ([793773e](https://github.com/prosody-events/prosody/commit/793773e754446a1e563d4d49e738b4b4fb322a2e))
* expose error types for public API consumption ([#139](https://github.com/prosody-events/prosody/issues/139)) ([1419471](https://github.com/prosody-events/prosody/commit/141947141539570932478f15981e3b08065919ef))
* expose rdkafka feature flags ([#140](https://github.com/prosody-events/prosody/issues/140)) ([e771ff2](https://github.com/prosody-events/prosody/commit/e771ff2c5fbeb4b0a3b4237072614baf322f85cd))
* expose rdkafka features ([#109](https://github.com/prosody-events/prosody/issues/109)) ([e2bef16](https://github.com/prosody-events/prosody/commit/e2bef16857d0ee4116851ee6b669094e621a5f83))
* expose source system ([#120](https://github.com/prosody-events/prosody/issues/120)) ([6d85e0a](https://github.com/prosody-events/prosody/commit/6d85e0a4432fb6e5fbf2da4158f57bd3a1b6af4e))
* grace period between dispatch halt and handler cancellation ([#160](https://github.com/prosody-events/prosody/issues/160)) ([7d4e2da](https://github.com/prosody-events/prosody/commit/7d4e2da3210773c991e455951f490f95fe5ebdb2))
* handle rebalance ([b20410e](https://github.com/prosody-events/prosody/commit/b20410e399361dc427eb29f02ea7a0ebdc97c0e1))
* **high-level:** introduce HighLevelClient and admin functionality ([#38](https://github.com/prosody-events/prosody/issues/38)) ([ab6ca00](https://github.com/prosody-events/prosody/commit/ab6ca008c53e46df94fd11e31015baec9d848f87))
* improve config error message and disable probes on mock ([#91](https://github.com/prosody-events/prosody/issues/91)) ([6d9b3e7](https://github.com/prosody-events/prosody/commit/6d9b3e79cb19266584f9456dae6b0ae1af7afc51))
* include more admin client features ([#125](https://github.com/prosody-events/prosody/issues/125)) ([babd895](https://github.com/prosody-events/prosody/commit/babd8952138501b66be40b9194732cda9727721a))
* keyed offset tracking ([cf0d388](https://github.com/prosody-events/prosody/commit/cf0d38815c7fb7cb5b24263cccdad2a2aa65b4e9))
* partition manager ([a443996](https://github.com/prosody-events/prosody/commit/a443996d1fa47ad5d916e8dcdc56b7811658164c))
* **partition-manager:** add deduplication ([#63](https://github.com/prosody-events/prosody/issues/63)) ([2bc8698](https://github.com/prosody-events/prosody/commit/2bc8698448ec3c786662cbab382c193c96c13933))
* per-timer-type semaphores ([#152](https://github.com/prosody-events/prosody/issues/152)) ([c44a2f3](https://github.com/prosody-events/prosody/commit/c44a2f3c0981e28adee9df768ccff1eadf5c2256))
* persistent deduplication middleware ([#155](https://github.com/prosody-events/prosody/issues/155)) ([4b1b38c](https://github.com/prosody-events/prosody/commit/4b1b38c0ef4f0cdeb07a181aa0c4481b0a275808))
* producer ([9e069ed](https://github.com/prosody-events/prosody/commit/9e069edad29ff0beaf4a4cc86f2761a2b10b4400))
* propagate trace spans on receive ([c10ec20](https://github.com/prosody-events/prosody/commit/c10ec2090d1e802a3079fe51ab0cd96f463c9e26))
* support regex topic subscriptions ([#115](https://github.com/prosody-events/prosody/issues/115)) ([55d72fe](https://github.com/prosody-events/prosody/commit/55d72fe7aa8727c5cb7d9001c010b87dea94fdfe))
* surface consumer configuration errors at subscribe time ([#153](https://github.com/prosody-events/prosody/issues/153)) ([4abc259](https://github.com/prosody-events/prosody/commit/4abc2597dd9f678f922178a08486570ec247d5fc))
* telemetry for message and timer lifecycles ([#148](https://github.com/prosody-events/prosody/issues/148)) ([22d853f](https://github.com/prosody-events/prosody/commit/22d853f651fa336db98f548ffb04514ad672e87f))
* timer cancel telemetry and emitter performance ([#151](https://github.com/prosody-events/prosody/issues/151)) ([98b8d88](https://github.com/prosody-events/prosody/commit/98b8d880341711708d5e6bc8973cabf1ed2a7b6c))
* **tracing:** allow subscriber layer injection ([#34](https://github.com/prosody-events/prosody/issues/34)) ([89bebf9](https://github.com/prosody-events/prosody/commit/89bebf97babc08681be7a3d4dd62cbe0079c9b4d))
* validate config ([401c671](https://github.com/prosody-events/prosody/commit/401c671f89fee8194ecb55f7d3234b7fe89268c1))


### Bug Fixes

* actually use stall threshold ([#79](https://github.com/prosody-events/prosody/issues/79)) ([1f2bdb9](https://github.com/prosody-events/prosody/commit/1f2bdb92cf6cc8bf247ca684f202cffcaaa4c5d7))
* add context to quickcheck failure messages in keyed test ([#144](https://github.com/prosody-events/prosody/issues/144)) ([e903208](https://github.com/prosody-events/prosody/commit/e903208f15188fac2347ec16d1e06fc9853ae9cf))
* add continue on error to CodeQL ([dd857da](https://github.com/prosody-events/prosody/commit/dd857dadc807fbffd5f26210592cbf58698950d2))
* add lock re-acquire with backoff ([06a8a8a](https://github.com/prosody-events/prosody/commit/06a8a8ada03c4d241273815e5a2e5f424766cd2e))
* add read permission ([04753e8](https://github.com/prosody-events/prosody/commit/04753e8ce8951b62a274b74275dc9f21d3eb4fb7))
* add test healthchecks ([#100](https://github.com/prosody-events/prosody/issues/100)) ([6994d21](https://github.com/prosody-events/prosody/commit/6994d21bc0af32d4a05353759ca1a0146fbb7135))
* allow CMAKE to be disabled ([#105](https://github.com/prosody-events/prosody/issues/105)) ([3cd8e50](https://github.com/prosody-events/prosody/commit/3cd8e50900b9ce6a2b54b7b3e0a213cf361c8aa2))
* avoid converting attributes to strings ([#50](https://github.com/prosody-events/prosody/issues/50)) ([d76ddcf](https://github.com/prosody-events/prosody/commit/d76ddcfa115f45a036e8052bd4c5beafbb207759))
* bootstrap ([#24](https://github.com/prosody-events/prosody/issues/24)) ([dc19841](https://github.com/prosody-events/prosody/commit/dc19841eb01c9fe1eaf513f50925f33d2a131a5b))
* build with cmake 4.x ([#89](https://github.com/prosody-events/prosody/issues/89)) ([5f69cef](https://github.com/prosody-events/prosody/commit/5f69ceffef9eb546002369ac25c2d9e2ad3d1620))
* cache parent context instead of span in Kafka loader ([#133](https://github.com/prosody-events/prosody/issues/133)) ([af75425](https://github.com/prosody-events/prosody/commit/af754255ede9d50db6a785b44e8cece9f74000a9))
* **cache:** eliminate value race in produce ([#94](https://github.com/prosody-events/prosody/issues/94)) ([46298bc](https://github.com/prosody-events/prosody/commit/46298bc59b6bac142f3c52e223445ddd4c2823db))
* check shutdown before attempting a retry ([#77](https://github.com/prosody-events/prosody/issues/77)) ([ff49466](https://github.com/prosody-events/prosody/commit/ff494669ffb5597f0c4fe0bfff60aee7a783b709))
* combined client changes ([#32](https://github.com/prosody-events/prosody/issues/32)) ([4cb4cc4](https://github.com/prosody-events/prosody/commit/4cb4cc4c55bb0d94325719486c0b52dde184af40))
* **consumer:** defer all offset commits to librdkafka ([#87](https://github.com/prosody-events/prosody/issues/87)) ([180f7a5](https://github.com/prosody-events/prosody/commit/180f7a5142e0410e65da384a60c0a4e8717657c3))
* **consumer:** don't commit final offsets during a rebalance ([#85](https://github.com/prosody-events/prosody/issues/85)) ([7892667](https://github.com/prosody-events/prosody/commit/789266734b319ec3794edb16cf294db63caae317))
* **consumer:** don’t try to commit an empty offset list ([#86](https://github.com/prosody-events/prosody/issues/86)) ([a23fe05](https://github.com/prosody-events/prosody/commit/a23fe05de5c7f74df1615fa69c15dd49ff03198b))
* **consumer:** push all messages through offset tracking at stream consumption time ([#83](https://github.com/prosody-events/prosody/issues/83)) ([06514a3](https://github.com/prosody-events/prosody/commit/06514a3e34bb59dfdcc1e8066915b232926a81ee))
* correct docs badge URL to include crate subdirectory ([#5](https://github.com/prosody-events/prosody/issues/5)) ([7afc3b0](https://github.com/prosody-events/prosody/commit/7afc3b0553ced393520bb556d129413f31b04da9))
* deadlock ([#75](https://github.com/prosody-events/prosody/issues/75)) ([ec8553f](https://github.com/prosody-events/prosody/commit/ec8553fb8df3586e7641c0703640d1aeb2b330b6))
* deduplicate messages on permanent handler errors ([#163](https://github.com/prosody-events/prosody/issues/163)) ([78bd402](https://github.com/prosody-events/prosody/commit/78bd402d52e311319861237bbe909f19ae2ad874))
* demote non-failure condition from error to debug ([#65](https://github.com/prosody-events/prosody/issues/65)) ([a32e5a9](https://github.com/prosody-events/prosody/commit/a32e5a90b86f9f9d6ccb0f0c2e4d647a859bc3a9))
* distinguish shutdown from cancellation in retry middleware ([#135](https://github.com/prosody-events/prosody/issues/135)) ([385ceea](https://github.com/prosody-events/prosody/commit/385ceea16e01fd7494970c6ab9a4038e2625c799))
* distributed lock for migrations ([4f1e608](https://github.com/prosody-events/prosody/commit/4f1e60837e7cc50ee1c95d966c43da958791520d))
* documentation link ([ec33041](https://github.com/prosody-events/prosody/commit/ec3304154c40c5f77257234889e056971bb25d86))
* don’t check for topics when mocking ([#44](https://github.com/prosody-events/prosody/issues/44)) ([64ef275](https://github.com/prosody-events/prosody/commit/64ef2751e1677b223549c9040f0457f985a93dff))
* don’t execute context methods when we’re shutting down ([#106](https://github.com/prosody-events/prosody/issues/106)) ([92dd604](https://github.com/prosody-events/prosody/commit/92dd604bbc07f526f5a2eacba3cc568c8e73d418))
* don’t fail if coverage artifact is not found ([6d1ff90](https://github.com/prosody-events/prosody/commit/6d1ff9043014264e30179ca6d52348fecdacd3d2))
* don't use SIMD JSON for ARM7 architectures ([#45](https://github.com/prosody-events/prosody/issues/45)) ([502bdf4](https://github.com/prosody-events/prosody/commit/502bdf4e5b128d64fd1e1a8fdfa84e4fe55b40c2))
* downgrade span parent extraction failures to debug level ([#132](https://github.com/prosody-events/prosody/issues/132)) ([32ea6c2](https://github.com/prosody-events/prosody/commit/32ea6c2e888d8cfabc68166b50f57007a17d11da))
* drop db span visibility to reduce noise ([#121](https://github.com/prosody-events/prosody/issues/121)) ([847b130](https://github.com/prosody-events/prosody/commit/847b13056227d3d682742df37a10c7e7f982164e))
* eager semaphore permit release ([#126](https://github.com/prosody-events/prosody/issues/126)) ([8ad5a30](https://github.com/prosody-events/prosody/commit/8ad5a30fbb04f097aed55d1a5574b47d88a14a0e))
* ensure mock server has topics ([#103](https://github.com/prosody-events/prosody/issues/103)) ([d7dfa47](https://github.com/prosody-events/prosody/commit/d7dfa47d31bdd942f14fe974bdfb5a2f85e42d20))
* env var description ([c028a35](https://github.com/prosody-events/prosody/commit/c028a352d10c40b4108a9bf349c62e04bc7b97a2))
* environment variable fallbacks ([#76](https://github.com/prosody-events/prosody/issues/76)) ([93c35b7](https://github.com/prosody-events/prosody/commit/93c35b72b412a721a29162c7d9455a874b7405c6))
* **failure:** ensure strategies short circuit upon shutdown ([#37](https://github.com/prosody-events/prosody/issues/37)) ([25e27bb](https://github.com/prosody-events/prosody/commit/25e27bbacafcaa87e7b0790dccabd433d778f0d0))
* graceful shutdown for slab_loader background task ([#143](https://github.com/prosody-events/prosody/issues/143)) ([87f6989](https://github.com/prosody-events/prosody/commit/87f6989a2d7c64d70f5772fcee5d3889e5d82875))
* improve test logging error handling and table naming ([#102](https://github.com/prosody-events/prosody/issues/102)) ([1332ce8](https://github.com/prosody-events/prosody/commit/1332ce89c6ff36aa29b0e3919f1d4bf4f882d6dd))
* increase default stall threshold from 15s to 5m ([#64](https://github.com/prosody-events/prosody/issues/64)) ([90806e0](https://github.com/prosody-events/prosody/commit/90806e0265511175d653b6d99c99be1b4028b1a5))
* invalidate context and spans after processing ([#124](https://github.com/prosody-events/prosody/issues/124)) ([494af97](https://github.com/prosody-events/prosody/commit/494af97f143492aafc72b9b81986077e6e05d1ff))
* library name collisions ([ab26721](https://github.com/prosody-events/prosody/commit/ab267219feec26313395d68f4dc964623d4aadd9))
* log OTEL initialization errors ([#108](https://github.com/prosody-events/prosody/issues/108)) ([833c6a2](https://github.com/prosody-events/prosody/commit/833c6a26349b0d77679928471713a18d90bf1e78))
* look for badge ([14da309](https://github.com/prosody-events/prosody/commit/14da3094dd23dae16aef7ce0a6600123ae8526b4))
* migrate CI, add release automation, and fix docs examples ([#1](https://github.com/prosody-events/prosody/issues/1)) ([4f480de](https://github.com/prosody-events/prosody/commit/4f480def038c86307d19e425e354b609d5a0fbe7))
* migrate release-please to v4 with config files ([#6](https://github.com/prosody-events/prosody/issues/6)) ([7a4581e](https://github.com/prosody-events/prosody/commit/7a4581e18e287bc2053b6fdaa15e50f84bb8980f))
* mocking ([#61](https://github.com/prosody-events/prosody/issues/61)) ([b20c9c5](https://github.com/prosody-events/prosody/commit/b20c9c514ecb85be4e084fddbdedebf8be1b44f8))
* prevent committing during a rebalance ([#81](https://github.com/prosody-events/prosody/issues/81)) ([a3254b6](https://github.com/prosody-events/prosody/commit/a3254b6ec2c01debbff2e91e62362c024cbd1ba3))
* prevent cross-topic key collisions from distorting decisions ([#131](https://github.com/prosody-events/prosody/issues/131)) ([09d9959](https://github.com/prosody-events/prosody/commit/09d99597638cd40b72cfa5616a243a83d099d092))
* prevent deleting new timer being scheduled ([#128](https://github.com/prosody-events/prosody/issues/128)) ([9aa4886](https://github.com/prosody-events/prosody/commit/9aa48866fbef8ac3c5020298056ee96a9b1e2fea))
* prevent false OffsetDeleted errors from concurrent loader requests ([#147](https://github.com/prosody-events/prosody/issues/147)) ([d21e645](https://github.com/prosody-events/prosody/commit/d21e645c8a644b28aa60178c7b8625201d93006f))
* **producer:** properly configure producer based on mode ([#36](https://github.com/prosody-events/prosody/issues/36)) ([2589701](https://github.com/prosody-events/prosody/commit/2589701836657a1e3d64fe2930b0f29618e8d5bd))
* promote transient errors to terminal during partition shutdown ([#158](https://github.com/prosody-events/prosody/issues/158)) ([72d85b5](https://github.com/prosody-events/prosody/commit/72d85b55bc4d5357f094b9b9cf8ed4b1250c2289))
* README example ([#40](https://github.com/prosody-events/prosody/issues/40)) ([68cf274](https://github.com/prosody-events/prosody/commit/68cf274b9d108cf37dae513a5e9023eb7d968d80))
* refresh trigger span on TriggerQueue dedup ([#159](https://github.com/prosody-events/prosody/issues/159)) ([d71d184](https://github.com/prosody-events/prosody/commit/d71d184da0c2b74c6cc534b3aeae476a0989d355))
* remove coverage report ([#16](https://github.com/prosody-events/prosody/issues/16)) ([5ba297f](https://github.com/prosody-events/prosody/commit/5ba297fe62a7019d47a415447ea41b839492b005))
* remove OTEL TLS ([#110](https://github.com/prosody-events/prosody/issues/110)) ([4fdbc3f](https://github.com/prosody-events/prosody/commit/4fdbc3f2eafd8ca0a0d771c7eb4c7997cb64dbf3))
* remove Scylla TLS for the time being due to build errors ([#99](https://github.com/prosody-events/prosody/issues/99)) ([6954b94](https://github.com/prosody-events/prosody/commit/6954b94c363c0a8d1cbc3e699dc4f6f22df26bc3))
* remove span enters that cross await points ([#122](https://github.com/prosody-events/prosody/issues/122)) ([d9066a1](https://github.com/prosody-events/prosody/commit/d9066a106a58711086af16b9c2b2a6c882036947))
* rename PROSODY_SCHEDULER_MAX_WAIT_SECS to PROSODY_SCHEDULER_MAX_WAIT ([#130](https://github.com/prosody-events/prosody/issues/130)) ([46fa386](https://github.com/prosody-events/prosody/commit/46fa386dcfe351ab83158e742a43d96ff0a4f7d4))
* resume partitions after rebalance assignment ([#136](https://github.com/prosody-events/prosody/issues/136)) ([5a178c4](https://github.com/prosody-events/prosody/commit/5a178c4aeacc13a55655d4025b2fb540b78a3f6b))
* rust-rdkafka[#681](https://github.com/prosody-events/prosody/issues/681) ([32608e7](https://github.com/prosody-events/prosody/commit/32608e7fff777577dab2f3d21e75376f6764b075))
* schedule the new timer before deleting the old timers ([#127](https://github.com/prosody-events/prosody/issues/127)) ([ddcaf00](https://github.com/prosody-events/prosody/commit/ddcaf00dfbb539b520430de19e0dc3f453cb9030))
* skip Inline→Overflow promotion when insert time matches existing inline ([#156](https://github.com/prosody-events/prosody/issues/156)) ([a3c04d6](https://github.com/prosody-events/prosody/commit/a3c04d661b93ed7332c55e3739f5de2745ae3e2a))
* support all OTEL protocols and use WebPKI certs ([#107](https://github.com/prosody-events/prosody/issues/107)) ([d11d1d3](https://github.com/prosody-events/prosody/commit/d11d1d3e74769e1f70327f12ae12c7507adbc15e))
* telemetry event_time uses millisecond precision with Z suffix ([#150](https://github.com/prosody-events/prosody/issues/150)) ([cc5f003](https://github.com/prosody-events/prosody/commit/cc5f003b0b957d1b9ba4fc55abd38dbf670ae85e))
* temporarily copy crate readme until bindings are complete ([#9](https://github.com/prosody-events/prosody/issues/9)) ([8b046a6](https://github.com/prosody-events/prosody/commit/8b046a60ab546d086f766ef8642906cef56635a4))
* terminate retry pauses on shutdown and extend max retry delay ([#66](https://github.com/prosody-events/prosody/issues/66)) ([31622c7](https://github.com/prosody-events/prosody/commit/31622c79fcd75bf9b76c8f2e84f183bdc46292ec))
* update badge URLs to prosody-events org ([#4](https://github.com/prosody-events/prosody/issues/4)) ([12eeae5](https://github.com/prosody-events/prosody/commit/12eeae586b3674d1b1002a64fe42d460e8d03a97))
* update repository references from cincpro to prosody-events ([#7](https://github.com/prosody-events/prosody/issues/7)) ([a0be2ef](https://github.com/prosody-events/prosody/commit/a0be2ef877e3dfcdbe0cc4d75bcc58056cfa24a6))
* upgrade codeql to v2 ([bac3032](https://github.com/prosody-events/prosody/commit/bac30321290c58eeefa6d594ef987a025ffb227f))
* upload codeql to v3 ([ef89ec3](https://github.com/prosody-events/prosody/commit/ef89ec3a08b02732492bf248bb5c6d0a988cbd1e))
* use a distributed lock for migrations ([#111](https://github.com/prosody-events/prosody/issues/111)) ([f9b1bec](https://github.com/prosody-events/prosody/commit/f9b1bec27e27ff2a46654a1d4315f9eecbd145ae))
* use GH_PAT org secret for release-please token ([#2](https://github.com/prosody-events/prosody/issues/2)) ([e8e2961](https://github.com/prosody-events/prosody/commit/e8e296130acffac30d799737176b3e1fdcf42e3a))
* use relative badge link ([#8](https://github.com/prosody-events/prosody/issues/8)) ([97f65ea](https://github.com/prosody-events/prosody/commit/97f65eaffaa8d7131dda7c9ec76d990c74b5dbeb))


### Performance Improvements

* **consumer:** add global bounded message buffering ([#117](https://github.com/prosody-events/prosody/issues/117)) ([3d7c42f](https://github.com/prosody-events/prosody/commit/3d7c42f73b82a27a51d2359272ea534dc45886c8))
* **consumer:** add maximum global concurrency bound ([#82](https://github.com/prosody-events/prosody/issues/82)) ([586f995](https://github.com/prosody-events/prosody/commit/586f9951c9dc4826493d01d871f8eb03157960cb))
* don’t commit on every poll ([aed8531](https://github.com/prosody-events/prosody/commit/aed853143a623d40697759098ef18fb49d480e69))
* ensure message processing cooperates with the Tokio scheduler ([#114](https://github.com/prosody-events/prosody/issues/114)) ([c69e5bd](https://github.com/prosody-events/prosody/commit/c69e5bdaec6b7068f05d48f33a68b1f7ef2bbcea))
* fix shutdown timeout short-circuit ([8cc8542](https://github.com/prosody-events/prosody/commit/8cc8542f6107997c0fe2f40eed5635cb267487cf))
* global dedup cache shared across partitions ([#157](https://github.com/prosody-events/prosody/issues/157)) ([6c0cbfe](https://github.com/prosody-events/prosody/commit/6c0cbfedc389a97f74a1d7353373d46a9e7b3232))
* introduce slab load jitter ([#112](https://github.com/prosody-events/prosody/issues/112)) ([dfeb633](https://github.com/prosody-events/prosody/commit/dfeb633d68e4059034ba04bb8d2e622c990e5afc))
* lower the cost of cloning messages ([#30](https://github.com/prosody-events/prosody/issues/30)) ([47bf4b8](https://github.com/prosody-events/prosody/commit/47bf4b88942f0bcce8da6086485241c922a84cdf))
* **producer:** remove duplicate key allocations ([#95](https://github.com/prosody-events/prosody/issues/95)) ([5938e0b](https://github.com/prosody-events/prosody/commit/5938e0bbc949dd8456605e0aa0045e57a0f4b79b))
* protect streams with coop scheduling ([#119](https://github.com/prosody-events/prosody/issues/119)) ([50d2530](https://github.com/prosody-events/prosody/commit/50d25304fa023bb3e7ae6d25af18d7a807cc35c8))
* reduce timer read tail latency and compaction load ([#141](https://github.com/prosody-events/prosody/issues/141)) ([51e2886](https://github.com/prosody-events/prosody/commit/51e288650019443e7db99cb235a374292ed26ca1))
* shutdown all partitions concurrently ([e3b4bfe](https://github.com/prosody-events/prosody/commit/e3b4bfe4d87124169891c0fd1d58f397c79dffae))
* use SIMD acceleration for JSON serialization and deserialization ([#41](https://github.com/prosody-events/prosody/issues/41)) ([a155dc6](https://github.com/prosody-events/prosody/commit/a155dc667106c8baf28d794ded5eb0b1fa3d6b32))
