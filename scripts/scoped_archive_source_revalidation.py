"""Exact source-only recertification of the published M3 10-Nov archive.

This is not a current-market refresh. The original producer manifest/proxy and
all transported data remain immutable; a new validator earns its own identity.
"""
from __future__ import annotations
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re

ACTIVE = 'archive_source_validation'
PREVIOUS = 'previous_archive_source_validation'
DATE = '2026-11-10'
RELEASE = '872abe41d48f818363d879cdf98b297d230377b4'
PROXY = 'outputs/microcap_top100_mom16_biweekly_live_v2_0_base_proxy_meta.json'
MODULE = 'scripts/scoped_archive_source_revalidation.py'
REVISION = 'exact_m3_20261110_boundary_coverage_source_only_v2'
STOCK_IDENTITY = '7c0520448023df5a318689e59fb9965da7da2239a069b55133f26c52951e5c04'
CHANGES = {
    'scripts/share_change_source.py': (
        '3dfb3ad0facbda0b8994af3767272fd35e22b16554993a60f6942d3e8bd74b7b',
        '21ab5dddc118c73d118a12dcc5250dde21b767677c42692b5a055eeed051f34f'),
    'data/share_source_future_plan_reviews.json': (
        '972825f752eb59d274c14e6a009f1b92cbed2e4a3f45ad5adce027f3b174e2e1',
        '86c353677ec35023bc451c00ca1fd13966f117e3cc7352958d0d5b28bd1b7aa8'),
}
# Exact additional notice-review source changes are frozen here before release.
# These are outside the 18 stock-reader sources and never relax stock/data gates.
ARCHIVE_INPUT_CHANGES = {'scripts/scoped_cap_boundary.py': (
    '54670a424c41ff805c7a87f1116e36975e782aaec56a7b8b7d8a7ea8b6831673',
    '595950c1532ce927b1707c572ebb88a86e790e8e72afbb342d9e9728f0d9fc11'),
 'data/st_notice_boundary_reviews/reviewed_20261111/AN202611091831555385.pdf': (None,
                                                                                '2143bce97172e6e08c77511595bc6cabdf066848e200ef81d9446f4d2db97752'),
 'data/st_notice_boundary_reviews/reviewed_20261111/AN202611091831555385.receipt.json': (None,
                                                                                         'd07edd7a4ae98745a048900c9c09410603442d4a5eef9861bd4c9138eca81b79'),
 'data/st_notice_boundary_reviews/reviewed_20261111/AN202611091831555385.txt': (None,
                                                                                '978fe28f63a32c05202a11a6e2ff7923fee49e8abf2232e98afb16949b7f2dde'),
 'data/st_notice_boundary_reviews/reviewed_20261111/AN202611101831591478.pdf': (None,
                                                                                'fdb4db69b3a44a76a1767cc5d623aaf4e80dfa50635b6232d012d2c65c373c59'),
 'data/st_notice_boundary_reviews/reviewed_20261111/AN202611101831591478.receipt.json': (None,
                                                                                         '40db3fc488973e48682bb39ce803b74c2c84988b071caf03361fd8212101d9ba'),
 'data/st_notice_boundary_reviews/reviewed_20261111/AN202611101831591478.txt': (None,
                                                                                'fba0b96c0b7960b7cb04befe6465978c7f1b52bcc5a34e53a10db896babf7438'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/manifest.json': (None,
                                                                             '63d24a06d80667512c74d0907a54ab6780c86ac740c1af391e8ad5f77ad339a3'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0001.json': (None,
                                                                              'deb66ba43b7a28f61de2620cf5423e20c3a2310ce880a87cce9399a73d969d81'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0001.receipt.json': (None,
                                                                                      '0342412c9cf7cbbbad3d34737db9eb440c2b84aa3d09308b36cef7e392c3ce5b'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0002.json': (None,
                                                                              '1dca559131d405ba8d846a69128bba54e13d9d7a6de4a0310975da7bb4dc4323'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0002.receipt.json': (None,
                                                                                      '47921ed707229371e69ed73e2ade1bf6b30256226917449c8a4d9991834fcc49'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0003.json': (None,
                                                                              '8a325933cb70b82f62e6e3631535432e22e20f0d0c4c76fa226d84036509ee04'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0003.receipt.json': (None,
                                                                                      '2fdfdd38c06293a209ada556e0d71bece31ac3095511b1dfbc81f1c112bb4df0'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0004.json': (None,
                                                                              '3dc8ee699968c0d43de1a4f4ae18ee22878c8d5dac446c96e8f95b1dc8cbf35a'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0004.receipt.json': (None,
                                                                                      '8b947013a274b0028f7f6b5548a77a6f500bd3263dc82e2a94f082d9a09fb0db'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0005.json': (None,
                                                                              'f26f381807247460e34d1e8a6356188c2a9aed2daf6f37d366ea4f5564f055c4'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0005.receipt.json': (None,
                                                                                      '3462d9645867350ddfbf8c00bf39a2b5199cd45aa2611d6c10033ad6fff3c9f2'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0006.json': (None,
                                                                              '58f55079ff2cf88e3dcefffa093331ccc1fb8d21aee0b3996842f5bb4f133785'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0006.receipt.json': (None,
                                                                                      '5e4d42cc135253063633329e17bcc75acdcd23e49aaaf68e2601c22f7c0589da'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0007.json': (None,
                                                                              'b9e6f643f1bbb7b51bee93d1d7fc0b433a847efe9d22719fbee90a0070dfe3be'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0007.receipt.json': (None,
                                                                                      'ba4337e710144ad8326de87523c5fdcfebdd94f6f430e1afd129d9c24dfe4ef1'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0008.json': (None,
                                                                              'b9b71ac1254722f5ee11065ea7a9876edaa2b56b2b080276938d4ca113a17440'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0008.receipt.json': (None,
                                                                                      'dd053fd34888dd81ee5b83a7d666010cb8aab5d22a45a821d9b6adca1ccdd20b'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0009.json': (None,
                                                                              '5965622f025ef3b90c336997ab185f677da33006b13a0a330473c2351e225305'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0009.receipt.json': (None,
                                                                                      'aaea4d6a0a70063e07bb71bce314cc7a13ef5560191f31877c729b4d36ea153c'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0010.json': (None,
                                                                              'a031aca975b5773e5348699a9611ff244f0c1fb40b06c9451e3f79a81ab7c4a9'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0010.receipt.json': (None,
                                                                                      '40726e9c60d17b16271790318662b230ab4597cd635984c113e8796780e0b3da'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0011.json': (None,
                                                                              'bd814482aa07cc85f5998bc9f6fdfad18f653f0c5a989b0016343bce56be4ec9'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0011.receipt.json': (None,
                                                                                      'bc25db3c0084dd85a6b1484d2c1240bfbb2067d77cdcef3cf8d59507f6b1a0ac'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0012.json': (None,
                                                                              '5f61ff6818eb615c7aa0b07d1e9a09412293a90c847e6fc355c86e7347ec6b50'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0012.receipt.json': (None,
                                                                                      'f6ed48642ff1297ac2e59e3e529e1b344fbfb0edd644eee9a7344073b0ca7568'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0013.json': (None,
                                                                              'ac0684d678d4809777ac901914142cf9cb3db04fef26fea573ec02b8a6c77810'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0013.receipt.json': (None,
                                                                                      '20ff05dbc74b4a173be234083ecd4b6fb20ad9164742c2510e83015add898939'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0014.json': (None,
                                                                              'daeba1794f53cc127b1a34b7cd38a11c731afa8dd374ac543611022931612934'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0014.receipt.json': (None,
                                                                                      '5859e7a2dd0a750b892194d0e221f89690141440de905f2fc058aa4b3c940079'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0015.json': (None,
                                                                              'ae68054e0ab5dcb38d71a171de60a34ac3fe93db4e2827fa503d1f10832e8ff1'),
 'data/st_notice_boundary_reviews/reviewed_20261111/current/page_0015.receipt.json': (None,
                                                                                      '7591bd44bccb781d57a55506546b35957a3ec65dc1a31a05e530bf09bf15e721'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/manifest.json': (None,
                                                                           'b3e1d3a426030940a04c26499da9d6959492444f452b849986f0257b80631e4d'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0001.json': (None,
                                                                            '7253873811fb13a8e4502573f8e7b573070f4d1e5a2d1bc28b695f52ab75971b'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0001.receipt.json': (None,
                                                                                    '780c63dc0bbdf194bc50f0c5ee1d3e715e9504480e563ad4f2117496668d5375'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0002.json': (None,
                                                                            '0583cfee4d424a90daeb33d28cecdb6af1a63b4212a0e16a98eb1ea699604045'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0002.receipt.json': (None,
                                                                                    '7d1949f5ac162810771a97afbad4d919303138df78e2eb600973b92b8c6cb277'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0003.json': (None,
                                                                            'c1fc51d7545251a1f9d248dc0b5e17431b9d687d6515545c82633eed3fe22299'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0003.receipt.json': (None,
                                                                                    '1d64e3c87f4d531475b21248eb1634ac45a7819cf012c987857311b6b17182be'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0004.json': (None,
                                                                            '9c6ce461bd3916b331c3affd5edf40ed4c8bb2016f6425870ce226e7e20bea39'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0004.receipt.json': (None,
                                                                                    '219ab14fcab9f07b7600707ed1ebe981434d1017b11b0c8faf1afaaa897241c7'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0005.json': (None,
                                                                            '872084d4dab505b90654b756b2dabaf8805c0240b87495380f1b29871dae7a3b'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0005.receipt.json': (None,
                                                                                    'a65011e9b001ca59b70ca501f64e66060e2d1caa17831d44b1397217500d84bf'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0006.json': (None,
                                                                            'ab87869ca7ed8e868a73a6c28f13392b1a91cc9a920fc84ff3c9dcde9001fe9e'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0006.receipt.json': (None,
                                                                                    '8e7bc17191143c8808f19f80eca94f87d683f55d99a1c2d4bcc09640091c7bb0'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0007.json': (None,
                                                                            'e52435f31eb0d9f50a0f0eb06ea1c7c3fcb25f6d1b0158ff4c1235de127e1340'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0007.receipt.json': (None,
                                                                                    'e46e9ca56f2b0b22d5a44e113812de32468976e8cb82b961ec862b0f500846cb'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0008.json': (None,
                                                                            '130c29419e11be915645762b6323614290c5477da9b9b28649d74499d26bb4e0'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0008.receipt.json': (None,
                                                                                    '68d277e6440e2aa0ca50d9ccfaf64353adf016fb5f3ae9bfcfc777873b4e6949'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0009.json': (None,
                                                                            '5c211ca9d3f9a50b16d564c4ce0889e8d01fefc5375f697a90f29313496cc531'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0009.receipt.json': (None,
                                                                                    'c7836e3e2e10d2964ceac65f31981f10fd457e19db58c4e8306b83bde1838ce5'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0010.json': (None,
                                                                            'a2b99083aecf217998a7021cf734bc7cfc48d20f6451f943fe784fd3cb011988'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0010.receipt.json': (None,
                                                                                    '7a9803f7324082b2b0c8d7513b31a44618431373eb4812afa490f2e836c99851'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0011.json': (None,
                                                                            'd3f20f59b07059316860962daf4145d42fedd1a6617708f1822bacbaff0ff4c0'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0011.receipt.json': (None,
                                                                                    'dbfa74608a970528a11ac61e15a5efc806f770dcbca2127708877f581eea19f3'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0012.json': (None,
                                                                            '69c7b1c426dbce158a365405f60fec2bb89ee12434f73bd0d53dec1c3f593140'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0012.receipt.json': (None,
                                                                                    '39d1a46ea396d3262df9bb6a3a4b19c5530dd84ad6f69410234660bd9a7baed7'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0013.json': (None,
                                                                            '42bb2c1af621294476ff5f4d9ab42506ff58725083c5a7e16cece5cfd270667a'),
 'data/st_notice_boundary_reviews/reviewed_20261111/day11/page_0013.receipt.json': (None,
                                                                                    '96fe17d7bff2b4eb9ec2e44e9840af42d9edc9c0d5a4f7d161782674b47ee931'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/manifest.json': (None,
                                                                              '6107c56ac30753f8f9e18560b2e48cce9b16ef56cfd546e88916840bbe57e547'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0001.json': (None,
                                                                               '5116a8a626ec82b971d483712780bdd30527155eac35f76986ac1d143e86d9b2'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0001.receipt.json': (None,
                                                                                       '713bbd32f5088a374fa2bb56b933bbbf32a551686fa97cc7cb6436b728f44494'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0002.json': (None,
                                                                               '15212de37ff2741a4e27b3da569d4b3bac29bd9eab88a76568eacd0b1d951dae'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0002.receipt.json': (None,
                                                                                       '0b699ab90bb4360816b079b0715b3df26b6b52a00c545ce2d44e1456cdb98b5e'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0003.json': (None,
                                                                               'b1de1f9a479fbdbd39afca217b87631880c8ce67288042ac5f11d2968d282584'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0003.receipt.json': (None,
                                                                                       'bb7cd4e14e87a61842b87916a012b75a9404652b6450230d03920c92905732af'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0004.json': (None,
                                                                               '591fd065f5905d7de61df7e39712da949e1fab9278e4f814bb3d75ad03725a70'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0004.receipt.json': (None,
                                                                                       'a1f252c7fb53355910712c3671db0a89d13ff85ff404dfc5ebe784e7e2418039'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0005.json': (None,
                                                                               'de4402d34637838e993ad35b0f7153557fe3b7cf3873ca482d1090810430000a'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0005.receipt.json': (None,
                                                                                       '67799e7e74e37dc346d0f088f7458b83ce1a016ea599337028031136aa170c99'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0006.json': (None,
                                                                               'db361f4202dc05846eb221d26150e23b7820a41c65cc7e1cffbb22a96a993853'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0006.receipt.json': (None,
                                                                                       '9796ed781798c487b8f5b7da3c1b82e99b107d7b6205949e9bd1695a6c390ff8'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0007.json': (None,
                                                                               '6e267d651161b640aa1ebe50d906c1c6361b180c1ecbfe40f4eddfc82f900a13'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0007.receipt.json': (None,
                                                                                       '6e38bebe1df46a7bca3034db4e7b4573fcb7c8fae2dd3d7b5031c62eb3ceff73'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0008.json': (None,
                                                                               'c28ed0bd29b512eb2d90ff7a1422dd30842f943d1bf2ab16e8d0e36ab52330c1'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0008.receipt.json': (None,
                                                                                       '8d37be1cb2688a6f84f30bbfe9a917fa727bf19805f628f043c55a530869db76'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0009.json': (None,
                                                                               'f8fd2e5b20b840d9b4c616495dc33e4f60fc804989cc03682cd1c759990cbc4f'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0009.receipt.json': (None,
                                                                                       '82d4cd7d48775239038699a19b327ce6ac5cf1cf107d588f9d7a08b580b365fb'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0010.json': (None,
                                                                               'b6f639cf0f0528f572920444f70ee2a27ab0cfabb132cd524eced973446081f6'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0010.receipt.json': (None,
                                                                                       '745ae2ca3eab0fbb4c2ef1e068921ecb06ba42abeab7d0a2bb688e2468487eca'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0011.json': (None,
                                                                               'ffa7db775a56eb8c7fae394c7c0977d2cd0a71fd47f33af178ca967d4e51fae5'),
 'data/st_notice_boundary_reviews/reviewed_20261111/previous/page_0011.receipt.json': (None,
                                                                                       '8a66ecd4b40d6cede9195c14c4fb60ac2bf20576849113a8f07445c2c8c7b643'),
 'data/st_notice_boundary_reviews/reviewed_20261111/registry.json': (None,
                                                                     'f1d8fcfcc665f3b4a705ff8d1cea2717170288fb98680426b9b98120a0f0c97d'),
 'scripts/scoped_source_consumer.py': ('7d8ee365a7ddf5028abbb3a55e0158ead0ec40b93115e719fd669ee7a4ab19c2',
                                       '880439e90f04bc4e47b6fade2a644e0a3a38331e9190ab5bd2c2e5b34393db95'),
 'scripts/st_notice_boundary_20261111.py': (None, '96063ff9cf2304dfb4253723851e3cca3b0dbc29b34d4b91a18de886c8414d8c'),
 'scripts/st_notice_boundary_review.py': ('790b76ffa40902d6876adcd003fb2cb340feb08d8ffaf05f7e18b0660a9b6d78',
                                          '4925c79eee7e0c41143a96b5d4c966aac20af7f66a5ae152efb19b3bc3387b92')}


def input_changes():
    return {**CHANGES, **ARCHIVE_INPUT_CHANGES}


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def original_manifest(raw):
    if digest(raw) not in BASELINE['original_manifest_sha256']:
        raise ValueError('Archive source revalidation requires an exact original M3 manifest')
    saved = json.loads(raw)
    if (saved.get('release_sha') != RELEASE or saved.get('expected_date') != DATE
            or saved.get('status') != 'complete' or saved.get('ok') is not True
            or saved.get('scope') != 'whole_workspace_delivery' or saved.get('errors') != []
            or ACTIVE in saved or PREVIOUS in saved):
        raise ValueError('Original M3 manifest identity differs')
    return saved


def checked_source(root):
    from scripts.top100_delivery import sha
    for name, (_, after) in input_changes().items():
        actual = (digest((Path(root)/name).read_bytes()) if name.startswith('data/st_notice_evidence/')
                  else sha(Path(root)/name))
        if actual != after:
            raise ValueError('Archive source revalidation has an unreviewed reader: ' + name)


def stock_compatible(recorded, current, root=None):
    """One exact old data/code state, and the actual unmodified 10-Nov proxy.

    The original caller in v2.0 need not change its strategy module. Its ROOT is
    used only for this exact reviewed legacy identity; the usual comparisons
    retain their existing pure path and all existing reviews stay unchanged.
    """
    if (recorded != BASELINE['stock_fingerprint'] or current.get('file_count') != recorded['file_count']
            or current.get('sha256') != recorded['sha256'] or set(current.get('code', {})) != set(recorded['code'])):
        return False
    changed = {n for n in recorded['code'] if recorded['code'][n] != current['code'][n]}
    if changed != set(CHANGES) or any((recorded['code'][n], current['code'][n]) != CHANGES[n] for n in changed):
        return False
    if root is None:
        import microcap_top100_mom16_biweekly_live_v2_0 as native
        root = native.ROOT
    try:
        raw = (Path(root)/PROXY).read_bytes()
        proxy = json.loads(raw)
        return (digest(raw) == BASELINE['proxy_sha256'] and proxy.get('end_date') == DATE
                and proxy.get('core_params', {}).get('stock_input_evidence_fingerprint') == recorded)
    except (OSError, ValueError, TypeError):
        return False


def require_transition(original, current):
    """Only the exact reviewed source edits plus this release-verified reader."""
    if current.get('ok') is not True or current.get('errors') != [] or current.get('expected_date') != DATE:
        raise ValueError('Archive source revalidation is limited to the complete 10-Nov archive')
    for key in ('scope', 'artifacts', 'streams'):
        if current.get(key) != original.get(key):
            raise ValueError('Archive source-only transition changed ' + key)
    old, new = original['inputs'], current['inputs']
    changes = input_changes()
    added = {n for n,pair in changes.items() if pair[0] is None}
    if set(new) != set(old) | {MODULE} | added or MODULE in old:
        raise ValueError('Archive source transition changed the input path set')
    changed = {n for n in old if old[n] != new[n]}
    if (changed != set(changes)-added or any((old[n], new[n]) != changes[n] for n in changed)
            or any(n in old or new[n] != changes[n][1] for n in added)):
        raise ValueError('Archive source transition changed unreviewed input bytes')
    if re.fullmatch('[0-9a-f]{64}', str(new[MODULE])) is None:
        raise ValueError('Archive source validation reader identity is missing')


def inventory_matches(entries):
    """Reuse hashes the original pack/restore already computes, with no reread."""
    data = [entry for entry in entries if entry['path'] != 'outputs/top100_delivery_manifest.json']
    if (len(data) != 170713 or [e['path'] for e in data] != sorted(set(e['path'] for e in data))
            or digest(json.dumps(data, sort_keys=True, separators=(',', ':')).encode()) != BASELINE['state_inventory_sha256']):
        raise ValueError('Archive source validation changed the original complete M3 data inventory')


def checked_receipt(receipt):
    keys = {'revision', 'archive_session', 'original_manifest_utf8', 'original_manifest_sha256',
            'original_release_sha', 'original_proxy_sha256', 'stock_fingerprint_sha256',
            'source_changes', 'validator_inputs', 'validation_release_sha', 'validated_at',
            'source_bundle_sha256', 'source_state_manifest_sha256', 'data_inventory_sha256', 'economic_csvs'}
    if not isinstance(receipt, dict) or set(receipt) != keys:
        raise ValueError('Incomplete archive source validation receipt')
    raw = receipt['original_manifest_utf8'].encode('utf-8')
    original = original_manifest(raw)
    if (receipt['revision'] != REVISION or receipt['archive_session'] != DATE
            or receipt['original_manifest_sha256'] != digest(raw) or receipt['original_release_sha'] != RELEASE
            or receipt['original_proxy_sha256'] != BASELINE['proxy_sha256']
            or receipt['stock_fingerprint_sha256'] != STOCK_IDENTITY
            or receipt['source_changes'] != {n:list(v) for n,v in input_changes().items()}
            or receipt['source_bundle_sha256'] != BASELINE['source_bundle_sha256']
            or receipt['source_state_manifest_sha256'] != BASELINE['state_manifest_sha256']
            or receipt['data_inventory_sha256'] != BASELINE['state_inventory_sha256']
            or receipt['economic_csvs'] != BASELINE['economic_csvs']
            or re.fullmatch('[0-9a-f]{40}', str(receipt['validation_release_sha'])) is None):
        raise ValueError('Archive source validation differs from the exact M3 review')
    stamp = datetime.fromisoformat(receipt['validated_at'])
    if stamp.tzinfo is None or not datetime.fromisoformat(DATE+'T15:00:00+08:00') <= stamp <= datetime.now(timezone.utc):
        raise ValueError('Archive source validation clock is invalid')
    expected = copy.deepcopy(original['inputs'])
    for name, (_, after) in input_changes().items():
        expected[name] = after
    inputs = receipt['validator_inputs']
    if (not isinstance(inputs, dict) or set(inputs) != set(expected) | {MODULE}
            or any(inputs[n] != value for n,value in expected.items())
            or re.fullmatch('[0-9a-f]{64}', str(inputs.get(MODULE, ''))) is None):
        raise ValueError('Archive validator inputs differ from the exact source transition')
    return original


def validate_active(root, current, saved):
    original = checked_document(saved)
    receipt = saved[ACTIVE]
    require_transition(original, current)
    if current['inputs'] != receipt['validator_inputs']:
        raise ValueError('Active archive source validation inputs changed')
    checked_source(root)
    if digest((Path(root)/PROXY).read_bytes()) != BASELINE['proxy_sha256']:
        raise ValueError('Archive source validation changed original proxy bytes')
    for name, expected in BASELINE['economic_csvs'].items():
        if digest((Path(root)/name).read_bytes()) != expected:
            raise ValueError('Archive source validation changed earned economic CSV: ' + name)
    return copy.deepcopy(receipt)


def checked_document(saved):
    original = checked_receipt(saved[ACTIVE])
    if (saved.get('archive_session') != DATE or saved.get('current_signal_ready') is not False
            or saved.get('signal_ready') is not False
            or saved.get('freshness') != 'verified_archive_only_requires_current_refresh'
            or saved.get('status') != 'complete' or saved.get('ok') is not True):
        raise ValueError('Archive source validation cannot claim current readiness')
    require_transition(original, saved)
    if saved['inputs'] != saved[ACTIVE]['validator_inputs']:
        raise ValueError('Active archive source validation inputs changed')
    return original


def annotate(root, report, saved, *, new_generation=False):
    """Keep an active archived certificate or retire it into flat ancestry."""
    if ACTIVE in saved and PREVIOUS in saved:
        raise ValueError('Ambiguous active and previous archive source validation')
    if ACTIVE in saved:
        if report.get('expected_date') == DATE:
            report[ACTIVE] = validate_active(root, report, saved)
            report.update(archive_session=DATE, current_signal_ready=False, signal_ready=False,
                          freshness='verified_archive_only_requires_current_refresh')
        elif new_generation and report.get('expected_date', '') > DATE:
            checked_receipt(saved[ACTIVE]); report[PREVIOUS] = copy.deepcopy(saved[ACTIVE])
        else:
            raise ValueError('An archived source certificate cannot certify another current session')
    elif PREVIOUS in saved:
        checked_receipt(saved[PREVIOUS])
        if report.get('expected_date', '') <= DATE:
            raise ValueError('Retired archive source validation requires a later native generation')
        report[PREVIOUS] = copy.deepcopy(saved[PREVIOUS])
    return report


def pending_ancestry(saved):
    receipt = saved.get(ACTIVE, saved.get(PREVIOUS))
    if receipt is None:
        return {}
    checked_receipt(receipt)
    return {PREVIOUS:copy.deepcopy(receipt)}


def data_inventory(root, report):
    from scripts import realtime_state_bundle as state, top100_cloud_delivery as cloud, top100_delivery as delivery
    root = state._extended(Path(root))
    names = sorted(set(state._iter_bundle_files(root)) |
                   ((set(report['inputs']) | set(report['artifacts'])) - set(cloud.SOURCE_FILES)))
    return [dict(path=n, bytes=(root/n).stat().st_size, sha256=state._sha256(root/n))
            for n in names if n != delivery.MANIFEST]


def recertify(root, expected_date):
    """Full archive validation; only the new manifest/backup may be written."""
    from scripts import top100_delivery as delivery, realtime_state_bundle as state
    root = Path(root).resolve()
    if expected_date != DATE:
        raise ValueError('Archive source revalidation is limited to M3 2026-11-10')
    lock = root/delivery.LOCK; lock.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    inode = os.fstat(descriptor).st_ino
    owner = str(os.getpid()).encode()
    retain_lock = False
    try:
        with os.fdopen(descriptor, 'wb') as stream:stream.write(owner)
        original_raw = (root/delivery.MANIFEST).read_bytes(); saved = json.loads(original_raw)
        original = original_manifest(original_raw) if ACTIVE not in saved else checked_receipt(saved[ACTIVE])
        checked_source(root)
        release = delivery.verify_release(root)
        if re.fullmatch('[0-9a-f]{40}', release) is None:
            raise ValueError('Archive validator requires an exact released commit')
        current = delivery.inspect_outputs(root, DATE)
        require_transition(original, current)
        if digest((root/PROXY).read_bytes()) != BASELINE['proxy_sha256']:
            raise ValueError('Archive source revalidation changed its original proxy')
        # This existing inventory also covers nonmember raw/cache leaves that
        # are not part of the portable bundle; they must not change locally.
        before = delivery._recertification_raw_identity(state._extended(root), current)
        archive = state.validate_archive_state(root, state._parse_date(DATE))
        if archive.get('ok') is not True or archive.get('errors'):
            raise ValueError('Native complete archive validation failed: '+str(archive.get('errors')))
        inventory_matches(data_inventory(root, current))
        final = delivery.inspect_outputs(root, DATE)
        require_transition(original, final)
        if (any(final[k] != current[k] for k in ('inputs','artifacts','streams'))
                or delivery._recertification_raw_identity(state._extended(root), final) != before
                or (root/delivery.MANIFEST).read_bytes() != original_raw
                or lock.stat().st_ino != inode or lock.read_bytes() != owner):
            raise ValueError('Archive source/data/manifest/owner changed during validation')
        if ACTIVE in saved:
            validate_active(root, final, saved)
            return {**saved, 'recertification_action':'already_complete_revalidated'}
        receipt = dict(revision=REVISION, archive_session=DATE, original_manifest_utf8=original_raw.decode(),
            original_manifest_sha256=digest(original_raw), original_release_sha=RELEASE,
            original_proxy_sha256=BASELINE['proxy_sha256'],stock_fingerprint_sha256=STOCK_IDENTITY,
            source_changes={n:list(pair) for n,pair in input_changes().items()}, validator_inputs=final['inputs'],
            validation_release_sha=release, validated_at=datetime.now(timezone.utc).isoformat(),
            source_bundle_sha256=BASELINE['source_bundle_sha256'],source_state_manifest_sha256=BASELINE['state_manifest_sha256'],
            data_inventory_sha256=BASELINE['state_inventory_sha256'],economic_csvs=copy.deepcopy(BASELINE['economic_csvs']))
        result = {**final, ACTIVE:receipt, 'status':'complete', 'release_sha':release,
                  'verified_at':receipt['validated_at'], 'archive_session':DATE,
                  'current_signal_ready':False,'signal_ready':False,'freshness':'verified_archive_only_requires_current_refresh'}
        validate_active(root, final, result)
        backup=root/'.codex_backups'/('archive_source_revalidation_'+digest(original_raw))/'manifest.original'
        backup.parent.mkdir(parents=True,exist_ok=True)
        try:
            with backup.open('xb') as stream:stream.write(original_raw)
        except FileExistsError:
            if backup.read_bytes()!=original_raw:raise ValueError('Archive source original backup differs')
        try:
            delivery.write_manifest(root,result)
            actual=(root/delivery.MANIFEST).read_bytes()
            if json.loads(actual)!=result:raise ValueError('Archive source manifest readback differs')
        except BaseException as commit_error:
            # Restore only while the original lock is still ours. Preserve the
            # immutable backup even if the operating system cannot roll back.
            try:
                if not lock.exists() or lock.stat().st_ino!=inode or lock.read_bytes()!=owner:
                    raise RuntimeError('Archive source validation lost its writer lock')
                temporary=(root/delivery.MANIFEST).with_suffix('.archive-source-rollback')
                temporary.write_bytes(original_raw);os.replace(temporary,root/delivery.MANIFEST)
                if (root/delivery.MANIFEST).read_bytes()!=original_raw:
                    raise RuntimeError('Archive source manifest rollback readback differs')
            except BaseException as rollback_error:
                retain_lock=True
                failure=backup.with_name('COMMIT_STATE_UNKNOWN.json')
                failure.write_text(json.dumps(dict(error=str(commit_error),rollback_error=str(rollback_error),
                                                  original_sha256=digest(original_raw)),indent=2))
                raise RuntimeError('Archive source COMMIT_STATE_UNKNOWN; inspect '+str(failure)) from commit_error
            raise
        return {**result,'recertification_action':'completed'}
    finally:
        if not retain_lock and lock.exists() and lock.stat().st_ino==inode and lock.read_bytes()==owner:lock.unlink()
BASELINE = {'source_bundle_sha256': '3d9aa183b614b272597bf81cfbd3b6fefdde021347fd36586e8047a6dc025e42',
 'source_bundle_bytes': 673074646,
 'state_manifest_sha256': 'b65cb02a8e14c871ae7b404def673b72ead0bf792654ab573c93df6cd2a4f4d5',
 'state_inventory_sha256': '42899f51e86c92ac4438bf25c45bd9419df3b9ba0c6997fa4fabe1c0af61d860',
 'original_manifest_sha256': ['141689e188a2d97bb39d641425cd3214dacfc01130cee43dc6c62a39e28aa29d',
                              '5b4af03dcbb4af7f25e7a724b3dc488e0020ef68f287a544a14c41465f771e25'],
 'proxy_sha256': 'e6d0075e7adc2a2b0631a1aab5ce6c613e54fc33598a0590340d1c8778183299',
 'stock_fingerprint': {'file_count': 16315,
                       'sha256': '88b1f3ba04dec2b0aa52a618c5cf853bf799ac2dd75121744dd1ebb57132aa51',
                       'code': {'scripts/exchange_calendar.py': '8c5f82f72fee80b8ffb5ca381bb894bdf82eeb267678800ba972bf9bad64abcb',
                                'scripts/stock_input_contract.py': 'b39102cf9a2be829dc1adf6c9305bf29fd5f2119d2ab42db91e363d87aec0e23',
                                'scripts/stock_return_basis.py': '4e67e8a31a216e6d0f8069fe869c9b6961ff52e503e1921a75e75e00c6046e59',
                                'scripts/stock_return_basis_store.py': 'fe1dfce2c961cff1e63ff9f2a1284f4b150897b83b521d85795a0aaf210fff39',
                                'scripts/stock_return_pipeline.py': '3c248e53500ea3952057f953bfe8e87e03f47eeb076d7b1a4a224ddf9b599ecb',
                                'scripts/tradeability_evidence.py': '9fbccd9b30003cc6bf590f678ba95da56e7bfc96f1488369d093285055159832',
                                'scripts/st_notice_archive.py': '625483138aebda935a852953a1645ec95eeff9fdf10ab259c463af198c7b7c9c',
                                'scripts/st_history_scope.py': 'e4ee3b06d91ba562c68354217def93d1d72482924138963ff34a706b9c15a063',
                                'scripts/st_notice_incremental.py': '0592bd87c515af2c7e819681e9c87714737d21c8d833050e4db0403625d2cdc3',
                                'scripts/st_notice_refresh.py': '0091610545f494d3c3e347596049bee39494fc9c4a4b557fa0dbdcb838443351',
                                'scripts/st_metadata_refresh.py': '14f7162e90accc385499155aed55ab540b293ec23a0ad6d357abcd212e64ca6a',
                                'scripts/stock_tail_returns.py': 'fc5875bdb53ed1786c7c851b8e3c9b8028f79f49a71c72cd4eb22c8f1c14d5c6',
                                'scripts/share_change_source.py': '3dfb3ad0facbda0b8994af3767272fd35e22b16554993a60f6942d3e8bd74b7b',
                                'scripts/share_unchanged_tail.py': '85bb77979ecdb179c2f3a71ce17261dd9c4bd0cd60b9a8c90c83d183a79146b2',
                                'data/share_source_future_plan_reviews.json': '972825f752eb59d274c14e6a009f1b92cbed2e4a3f45ad5adce027f3b174e2e1',
                                'scripts/sina_share_history.py': 'fe726fa2ba56a8321d2ded915ed8c20d71a1666b4eef375ab3707a6c0380c702',
                                'scripts/stock_daily_inputs.py': 'd5a7f4b496798ef4e2c834b9317ebf435cf8600de0f880a202a4fc5ce6e381f5',
                                'scripts/stock_daily_alternative.py': '95dad1ffa24d13903fe845990afb2e370308ed44284f4428fd900c634a04be7d'}},
 'economic_csvs': {'outputs/wind_microcap_top_100_biweekly_thursday_16y_cached.csv': '91e65fb772c28a65c4bc98d103218affda7f0e7952a08608bbaeb37425b78932',
                   'outputs/microcap_top100_mom16_plain_fixed1_v2_0_costed_nav.csv': '36d6afb692ea3795deeb803fca2b0063bd21b808536b329bc04f1d50a0c6f271',
                   'outputs/microcap_top100_mom16_lb25_hl2p5_r2off_eb0p08_no_overheat_exec0p8_v2_3_costed_nav.csv': '4c998666ad6d863e083ef24174e9974b32fe5ce78bf5d0dd47f3d41e96920d84',
                   'outputs/microcap_top100_mom16_lb20_hl3_entry0_exit0_no_targetvol_v2_5_costed_nav.csv': '89ba1ee56fb09b26073d47834811a0b9e675359299a0aa95a0fd8266f9522208',
                   'outputs/microcap_top100_mom16_biweekly_live_v2_0_performance_nav.csv': 'ca3d407833224571cec8505625be9be4827b327abbb1372ce8b1b6b23648545f',
                   'outputs/microcap_top100_mom16_biweekly_live_v2_3_performance_nav.csv': '5f3fdea152fd6d7356e1dbaa4dc35962e1355528396c8ea2a84fb7bc8311a64d',
                   'outputs/microcap_top100_mom16_biweekly_live_v2_5_performance_nav.csv': '5f7755df4f08ec55c48bac8070d79dd1a50a266a1d901a5e5fdde97f83d54452'}}
