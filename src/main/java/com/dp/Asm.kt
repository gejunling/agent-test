package com.dp


import com.vladsch.kotlin.jdbc.using
import kotliquery.queryOf
import kotliquery.sessionOf
import mu.KotlinLogging
import oracle.jdbc.OracleTypes
import java.sql.Connection


const val MAX_NUM_AU_DISK = 16
const val MAX_NUM_ASM_OPEN = 32

enum class AsmWorkMode {
    RAW, DB
}

private val logger = KotlinLogging.logger {}

/** 全局变量存储 FD */
val asmfd: MutableList<Int> = MutableList(THREAD_NUM_MAX) { 0 }

/** 全局表里存储 [AsmDbFile] */
val asmdbfs: MutableList<MutableList<AsmDbFile>> = MutableList(THREAD_NUM_MAX) {
    MutableList(MAX_NUM_ASM_OPEN) { AsmDbFile() }
}

val asmFiles: MutableList<MutableList<AsmFile>> = MutableList(THREAD_NUM_MAX) {
    MutableList(MAX_NUM_ASM_OPEN) { AsmFile() }
}


data class Asm(
    val asm_login: String,
    val asm_oracle_sid: String,
    val asm_oracle_home: String,
    val asm_mode: String,
    val sub_asm: List<SubAsm>)

class SubAsm(
    val asm_device_id: Int,
    val asm_disk: String,
    val asm_dev: String
)

/**
 * 将当前块，转换为AU块，得到当前偏离量在第几个AU块，在该AU块的偏移位置，AU块偏移后剩下的长度
 *
 * @param off 当前块的偏移位置
 * @return <当前偏离量在第几个AU块,在该AU块的偏移位置,AU块偏移后剩下的长度>
 */
fun AsmFile.getAsmFileAuOff(off: Int): Triple<Int, Int, Int> {
    val strpidx: Int
    val au_idx8: Int
    val au_loop: Int
    // return stuff
    val au_idx: Int
    val au_off: Int
    val len: Int

    val au_off8: Int
    val strpoff: Int
    if (strpwd > 1) {
        if (OFF_MORE) {
            au_loop = au_size * strpwd
            au_idx8 = off / au_loop * strpwd
            au_off8 = off % au_loop
            strpidx = au_off8 / au_size
            strpoff = off % strpsz
            au_idx = au_idx8 + (au_off8 % au_size) / strpsz
            au_off = strpidx * strpsz + strpoff
            len = strpsz - strpoff
        } else {
            au_idx = off / au_size
            au_off = off % au_size
            len = strpsz - off % au_size % strpsz
        }
    } else {
        au_idx = off / au_size
        au_off = off % au_size
        len = au_size - au_off
    }
    return Triple(au_idx, au_off, len)
}

val EXADATA by lazy { CONFIG[ParamSpec.exadata] }
val OFF_MORE by lazy { CONFIG[ParamSpec.offmore] }

fun readAsm(rac: Int, dst: ByteArray, cnt: Int, mode: AsmWorkMode): Int {
    return when (mode) {
        AsmWorkMode.RAW -> readRawAsm(rac, dst, cnt)
        AsmWorkMode.DB -> readDbAsm(rac, dst, cnt)
    }
}

fun closeAsm(rac: Int, mode: AsmWorkMode) {
    when (mode) {
        AsmWorkMode.RAW -> closeRawAsm(rac)
        AsmWorkMode.DB -> closeDbAsm(rac)
    }
}

fun openAsm(rac: Int, name: String, mode: AsmWorkMode): Int {
    return when (mode) {
        AsmWorkMode.RAW -> openRawAsm(rac, name)
        AsmWorkMode.DB -> openDbAsm(rac, name)
    }
}

/**
 *  raw模式下，读取asm文件
 *
 * @param cnt 要读取的大小
 * @param dst
 * @return
 */
fun readRawAsm(rac: Int, dst: ByteArray, cnt: Int): Int {
    val f = findAsmFile(rac)
    var size = cnt
    var off = f.off
    if (size < 1 || off >= f.file_size) {
        return 0
    }
    if (f.file_size - off < size) {
        size = f.file_size - off
    }
    var au: AU
    var readBytes = 0
    var leftBytes = size
    var auNo: Int
    var disk: AsmDisk
    var auBlockOff: Int
    var auIdx: Int
    var len1: Int
    var len2: Int
    var auOff: Long
    while (leftBytes > 0) {
        val (idx, blkOff, len) = f.getAsmFileAuOff(off)
        auIdx = idx
        auBlockOff = blkOff
        len1 = len
        au = f.aus[auIdx]
        if (au.size <= 0) {
            return 0
        }
        if (len1 > leftBytes) {
            len1 = leftBytes
        }
        var success = false
        for (a in au) {
            disk = a.disk
            auNo = a.au_num
            auOff = auNo.toLong() * f.au_size + auBlockOff
            len2 = readFromRaw(disk, auOff, dst, readBytes, len1)
            if (len2 <= 0) {
                continue
            }
            off += len2
            leftBytes -= len2
            readBytes += len2
            success = true
            break
        }
        if (success) {
            continue
        } else {
            throw Exception("can not read asm raw device('${f.filename}', off=${off} len=${len1})")
        }
    }
    f.off = off
    return readBytes
}

/** raw模式下，读取操作 */
fun readFromRaw(disk: AsmDisk, off: Long, dst: ByteArray, dstOff: Int, len: Int): Int {
    if (disk.fd < 0) {
        disk.fd = openLocalFile(disk.dev)
    }
    seekLocal(disk.fd, off, Whence.SEEK_SET)
    val len2 = readLocal(disk.fd, dstOff, dst, len)
    if (len2 <= 0) {
        throw Exception("error to read(fd=${disk.fd},len=${len})")
    }
    return len2
}

/**
 * Seek raw asm
 *
 * @param off 偏移量
 * @param w 从哪个位置开始偏移
 * @return seek后实际偏移量
 */
fun seekRawAsm(rac: Int, off: Int, w: Whence): Int {
    val f = findAsmFile(rac)
    when (w) {
        Whence.SEEK_SET -> f.off = off
        Whence.SEEK_CUR -> f.off += off
        Whence.SEEK_END -> f.file_size - off
    }
    return f.off
}

fun closeRawAsm(rac: Int) {
    if (asmfd[rac] >= 0) {
        asmFiles[rac][asmfd[rac]] = AsmFile()
        asmfd[rac] = -1
    }
}

/**
 * db模式下，读取文件操作
 * ```
 *  val sql = "declare
 *  v_type number:=2;
 *  v_lblksize number:=512;
 *  v_handle number;
 *  v_pblksz number;
 *  v_fsz number;
 *  v_buf raw(8192);
 *  v_off number:=50;
 *  v_ref SYS_REFCURSOR;
 *  begin
 *      dbms_diskgroup.open('+DATA/ORCL/archivelog/2020_11_24/thread_1_seq_19.285.1057323603','r',v_type,v_lblksize,v_handle,v_pblksz,v_fsz);
 *      dbms_diskgroup.read(v_handle, v_off, v_pblksz, v_buf);
 *      dbms_output.put_line('Content: '||v_buf);
 *      dbms_diskgroup.close(v_handle);
 *  open v_ref for  select v_buf buuf from dual; ? := v_ref;
 *  end;
 * ```
 * @param rac
 * @param dst
 * @param cnt 要读取的字节数
 * @return
 */
fun readDbAsm(rac: Int, dst: ByteArray, cnt: Int): Int {
    var size = cnt
    val f = findAsmDbFile(rac)
    var rc: Int
    var readBytes = 0
    var dstOff = 0
    if (f.off == 0 && size >= f.block_size) { //当前偏移量为0，且读取数据长度超过1块的长度
        f.off = f.block_size  //偏移量设置为块末尾位置
        if (ORA_VER == 11) {
            if (EXADATA) {
                dst[0] = 0x0
                dst[1] = 0x22
                dst[2] = 0x0
                dst[3] = 0x0
                dst[4] = 0x0
                dst[5] = 0x0
                dst[6] = 0xC0.toByte()
                dst[7] = 0xFF.toByte()
                dst[8] = 0x0
                dst[9] = 0x0
                dst[10] = 0x0
                dst[11] = 0x0
                dst[12] = 0x0
                dst[13] = 0x0
                dst[14] = 0
                dst[15] = 0
                dst[16] = 0xC7.toByte()
                dst[17] = 0x49
                dst[18] = 0
                dst[19] = 0

                // block_size: 0x200=512
                dst[20] = 0
                dst[21] = 0x02
                dst[22] = 0
                dst[23] = 0

                // block_cnt: 0x400000=4194304
                dst[24] = 0
                dst[25] = 0
                dst[26] = 0x40
                dst[27] = 0

                // pos:0x1c end_type:0 int_byte_cnt:4
                dst[28] = 0x7D
                dst[29] = 0x7C
                dst[30] = 0x7B
                dst[31] = 0x7A
            } else {
                dst[0] = 0x0
                dst[1] = 0x22
                dst[2] = 0x0
                dst[3] = 0x0
                dst[7] = 0x0
                dst[6] = 0x0
                dst[5] = 0xC0.toByte()
                dst[4] = 0xFF.toByte()
                dst[8] = 0x0
                dst[9] = 0x0
                dst[10] = 0x0
                dst[11] = 0x0
                dst[12] = 0x0
                dst[13] = 0x0
                dst[14] = 0
                dst[15] = 0
                dst[16] = 0xF9.toByte()
                dst[17] = 0xE4.toByte()
                dst[18] = 0
                dst[19] = 0

                // block_size: 0x200=512
                dst[20] = 0
                dst[21] = 0
                dst[22] = 0x02
                dst[23] = 0

                // block_cnt: 0x19000=50M
                dst[24] = 0
                dst[25] = 0x0
                dst[26] = 0x02
                dst[27] = 0x0

                // pos:0x1c end_type:1 int_byte_cnt:4
                dst[31] = 0x7D
                dst[30] = 0x7C
                dst[29] = 0x7B
                dst[28] = 0x7A
            }
        } else {
            if (EXADATA) {
                dst[0] = 0x0
                dst[1] = 0x22
                dst[2] = 0x0
                dst[3] = 0x0
                dst[7] = 0x0
                dst[6] = 0x0
                dst[5] = 0xC0.toByte()
                dst[4] = 0xFF.toByte()
                dst[8] = 0x0
                dst[9] = 0x0
                dst[10] = 0x0
                dst[11] = 0x0
                dst[12] = 0x0
                dst[13] = 0x0
                dst[14] = 0
                dst[15] = 0
                dst[16] = 0xF9.toByte()
                dst[17] = 0xE4.toByte()
                dst[18] = 0
                dst[19] = 0
                dst[20] = 0
                dst[21] = 0
                dst[22] = 0x02
                dst[23] = 0
                dst[24] = 0
                dst[25] = 0x0
                dst[26] = 0x02
                dst[27] = 0x0
                dst[31] = 0x7D
                dst[30] = 0x7C
                dst[29] = 0x7B
                dst[28] = 0x7A
            } else {
                dst[0] = 0x0
                dst[1] = 0x22
                dst[2] = 0x0
                dst[3] = 0x0
                dst[4] = 0x0
                dst[5] = 0x0
                dst[6] = 0xC0.toByte()
                dst[7] = 0xFF.toByte()
                dst[8] = 0x0
                dst[9] = 0x0
                dst[10] = 0x0
                dst[11] = 0x0
                dst[12] = 0x0
                dst[13] = 0x0
                dst[14] = 0
                dst[15] = 0
                dst[16] = 0xC7.toByte()
                dst[17] = 0x49
                dst[18] = 0
                dst[19] = 0
                dst[20] = 0
                dst[21] = 0x02
                dst[22] = 0
                dst[23] = 0
                dst[24] = 0
                dst[25] = 0x90.toByte()
                dst[26] = 0x01
                dst[27] = 0
                dst[28] = 0x7D
                dst[29] = 0x7C
                dst[30] = 0x7B
                dst[31] = 0x7A
            }
        }
        if (f.block_size >= size) { // 如果要读取数据块长度就是dbf的1个块的长度，直接返回
            rc = size
            return rc
        }
        size -= f.block_size
        readBytes = f.block_size
        dstOff += f.block_size
    }
    val sql = "begin dbms_diskgroup.read(?, ?, ?, ?);end;"
    f.con!!.prepareCall(sql)
        .use { cs ->
            var rd = 0
            while (rd < size) {
                val blockNo = f.off / f.block_size
                val rdLen = kotlin.math.min(
                    kotlin.math.min(size - rd, 30 * 1024 / REDO_BLOCK_SIZE * REDO_BLOCK_SIZE),
                    f.file_size - f.off)
                cs.setInt(1, f.file_handle)
                cs.setInt(2, blockNo)
                cs.setInt(3, rdLen)
                cs.registerOutParameter(4, OracleTypes.BINARY)
                cs.execute()
                val raw = cs.getBytes(4)
                System.arraycopy(raw, 0, dst, dstOff, raw.size)
                val olen = raw.size / f.block_size * f.block_size
                f.off += olen
                rd += olen
                dstOff += olen
            }
            rc = rd + readBytes
            return rc
        }

}

fun seekDbAsm(rac: Int, off: Int, w: Whence): Int {
    val f = findAsmDbFile(rac)
    //必须是块的整数倍
    if (off % f.block_size > 0) {
        throw Exception("can not seek to non-block-margin for a ASM-DB-file")
    }
    when (w) {
        Whence.SEEK_SET -> f.off = off
        Whence.SEEK_CUR -> f.off += off
        Whence.SEEK_END -> f.off = f.file_size - off
    }
    return f.off
}

fun findAsmFile(rac: Int): AsmFile = asmFiles[rac][asmfd[rac]]
fun findAsmDbFile(rac: Int): AsmDbFile = asmdbfs[rac][asmfd[rac]]

/**
 * 从数据库查询到asm文件类型，块数，每块大小
 *
 * @param name 文件名
 * @return <文件类型, 块数, 块大小>
 */
fun getDbAsmFileAttr(name: String): Triple<String, Int, Int> {
    val sql = "begin dbms_diskgroup.getfileattr(?, ?, ?, ?); end;"
    OracleDatabase.asm.connection.use {
        val cs = it.prepareCall(sql)
        cs.setString(1, name)
        cs.registerOutParameter(2, OracleTypes.CHAR)
        cs.registerOutParameter(3, OracleTypes.INTEGER)
        cs.registerOutParameter(4, OracleTypes.INTEGER)
        cs.execute()
        return Triple(cs.getString(2), cs.getInt(3), cs.getInt(4))
    }
}

fun openDbAsm(rac: Int, name: String): Int {
    val (fileType, blocks, blkSiz) = getDbAsmFileAttr(name)
    val sql = """
        begin
            dbms_diskgroup.open(?,?,?,?,?,?,?);
        end;
    """.trimIndent()
    val con = OracleDatabase.asm.connection
    con.prepareCall(sql)
        .use { cs ->
            cs.setString(1, name)
            cs.setString(2, "r")
            cs.setInt(3, fileType.toInt())
            cs.setInt(4, blkSiz)
            cs.registerOutParameter(5, OracleTypes.INTEGER)
            cs.registerOutParameter(6, OracleTypes.INTEGER)
            cs.registerOutParameter(7, OracleTypes.INTEGER)
            cs.execute()
            val file = AsmDbFile(name = name,
                file_type = fileType,
                blocks = cs.getInt(7),
                block_size = blkSiz,
                is_used = true,
                file_handle = cs.getInt(5),
                physical_block_size = cs.getInt(6),
                con = con)
            file.blocks++
            var rc = -1
            for (i in 0 until MAX_NUM_ASM_OPEN) {
                if (!asmdbfs[rac][i].is_used) {
                    file.file_size = file.block_size * file.blocks
                    asmdbfs[rac][i] = file
                    rc = i
                    break
                }
            }
            if (rc < 0) {
                throw Exception("Too many open asm files (>${MAX_NUM_ASM_OPEN}) ")
            }
            asmfd[rac] = rc
            return rac
        }
}

/** db模式下，关闭文件操作 */
fun closeDbAsm(rac: Int) {
    val sql = "begin dbms_diskgroup.close(?); end;"
    val f = findAsmDbFile(rac)
    if (!f.is_used) {
        return
    }
    try {
        f.con!!.use {
            val cs = f.con!!.prepareCall(sql)
            cs.setInt(1, findAsmDbFile(rac).file_handle)
            cs.execute()
        }
    } finally {
        f.is_used = false
        asmfd[rac] = -1
    }
}

/** raw模式下，获取索引号对应的文件的大小 */
fun statAsm(rac: Int, workMode: AsmWorkMode): Int {
    return when (workMode) {
        AsmWorkMode.RAW -> findAsmFile(rac).file_size
        AsmWorkMode.DB -> findAsmDbFile(rac).file_size
    }
}

/**
 * raw模式下，打开文件操作,获取filename的文件信息并保存到asmfiles全局变量中,返回保存在
 *
 * @param rac
 * @param name 文件名
 * @return [asmFiles] 中的索引
 */
fun openRawAsm(rac: Int, name: String): Int {
    // TODO IS_TGT
    getAsmDisks(rac)
    val (group_no, file_no) = getAsmGroupNoFileNo(name)
    using(sessionOf(OracleDatabase.asm)) { session ->
        val asmFile = session.run(
            queryOf("""
                    select g.group_number, g.allocation_unit_size, f.number_kffil, f.blksiz_kffil, f.blkcnt_kffil, f.filsiz_kffil, f.strpwd_kffil 
                    from v${'$'}asm_diskgroup g, x${'$'}kffil f, v${'$'}asm_alias a 
                    where g.group_number=f.group_kffil and g.group_number=a.group_number and f.number_kffil=a.file_number
                     and g.group_number=${group_no} and a.file_number=${file_no}
                """.trimIndent()).map {
                val strpwd = it.int(7)
                val auSize = it.int(2)
                val strpsz = if (strpwd > 1) {
                    auSize / strpwd
                } else {
                    auSize
                }
                AsmFile(is_used = true,
                    group_no = it.int(1),
                    au_size = auSize,
                    file_no = it.int(3),
                    block_size = it.int(4),
                    blocks = it.int(5),
                    file_size = it.int(6),
                    strpwd = strpwd,
                    strpsz = strpsz)
            }.asSingle)
        val aus: Array<AU> = Array { mutableListOf() }
        if (session.run(queryOf(
                """
                         select group_kffxp,disk_kffxp,au_kffxp,xnum_kffxp 
                            from x${'$'}kffxp 
                            where group_kffxp=${asmFile!!.group_no} and number_kffxp=${asmFile!!.file_no} and xnum_kffxp<1000000  
                            order by xnum_kffxp,group_kffxp,disk_kffxp    
                        """.trimIndent()
            ).map {
                val groupNo = it.int(1)
                val diskNo = it.int(2)
                val auNum = it.int(3)
                val auNo = it.int(4)
                val au = aus[auNo]
                if (au.size < MAX_NUM_AU_DISK) {
                    val disk =
                        asmdisks[rac]!!.first { d -> d.group_no == groupNo && d.disk_no == diskNo }
                    au.add(Au(disk = disk, au_num = auNum))
                }
            }.asList)
                .isEmpty()) {
            throw Exception("asm file $name has not extent map")
        }
        val file = asmFile!!
        file.filename = name
        file.off = 0
        file.aus = aus
        var rc = -1
        for (i in 0 until MAX_NUM_ASM_OPEN) {
            if (!asmFiles[rac][i].is_used) {
                asmFiles[rac][i] = file
                rc = i
                break
            }
        }
        asmfd[rac] = rc
    }
    return rac
}

/** raw模式下，获取组号及文件号 */
fun getAsmGroupNoFileNo(name: String): Pair<Long, Long> {
    if (!name.startsWith("+")) {
        throw Exception("asm file: ${name} should start with '+'.")
    }
    val tokens = name.substring(1)
        .split("/")
    if (tokens.size < 2) {
        throw Exception("split wrong ${name}")
    } //根据文件路径的GROUP4，从数据库查询到组号，并传出参数组号及查询组号的sql
    var groupNo: Long? = null
    using(sessionOf(OracleDatabase.asm)) { session ->
        groupNo = session.run(
            queryOf("select to_char(group_number) from v${'$'}asm_diskgroup where upper(name)='${tokens[0].toUpperCase()}'").map {
                it.string(1)
                    .toLong()
            }.asSingle)
    }
    var refNum = groupNo!! shl 24
    var fileNo: Long? = null
    for (i in 1 until tokens.size) {
        using(sessionOf(OracleDatabase.asm)) { session ->
            session.run(
                queryOf("select number_kfals, refer_kfals from x${'$'}kfals where parent_kfals=${refNum} and upper(name_kfals)='${tokens[i].toUpperCase()}'").map {
                    fileNo = it.long(1)
                    refNum = it.long(2)
                }.asSingle)
        }
    }
    return groupNo!! to fileNo!!
}

val asmdisks: MutableList<List<AsmDisk>?> = MutableList(THREAD_NUM_MAX) {
    mutableListOf()
}

/** raw模式下，获取磁盘信息 */
fun getAsmDisks(rac: Int) {
    if (asmdisks[rac]!!.isNotEmpty()) {
        return
    }
    var disks: List<AsmDisk>? = null
    using(sessionOf(OracleDatabase.asm)) { session ->
        disks = session.run(
            queryOf("select GROUP_NUMBER,DISK_NUMBER, path from v${'$'}asm_disk").map {
                AsmDisk(group_no = it.int(1), disk_no = it.int(2), path = it.string(3))

            }.asList)
    }
    if (disks == null || disks!!.isEmpty()) {
        throw Exception("Failed to get asm disk info")
    }
    asmdisks[rac] = disks!!
    for (disk in asmdisks[rac]!!) {
        for (map in devmap[rac]) {
            if (disk.path == map.disk) {
                disk.dev = map.dev
                break
            }
        }
    }
    for (disk in asmdisks[rac]!!) {
        disk.fd = -1
        if (disk.dev.isEmpty()) { //如果dev在asm.cfg中没有配置，则直接是ASM_DISK_STRU中磁盘路径
            disk.dev = disk.path
        }
    }
}

val devmap: MutableList<List<AsmDevMap>> = MutableList(THREAD_NUM_MAX) {
    mutableListOf()
}

fun getAsmMapCfg(rac: Int, asm: Asm) {
    devmap[rac] = asm.sub_asm.map {
        AsmDevMap(disk = it.asm_disk, dev = it.asm_dev)
    }
        .toList()
}

data class AsmFile(var is_used: Boolean = false,
                   var group_no: Int = 0,
                   val au_size: Int = 0,
                   val file_no: Int = 0,
                   val block_size: Int = 0,
                   val blocks: Int = 0,
                   val file_size: Int = 0,
                   var off: Int = 0,
                   var stripe_count: Int = 0,
                   var strpwd: Int = 0,
                   var strpsz: Int = 0,
                   var aus: Array<AU> = Array { mutableListOf() },
                   var filename: String = "")

/**
 * Asm db file
 *
 * @property is_used 是否已被 open
 * @property index
 * @property file_type 文件类型
 * @property blocks 块数
 * @property block_size 块大小
 * @property file_handle 文件句柄
 * @property physical_block_size 物理块大小
 * @property off 偏移量
 * @property file_size 文件大小
 * @constructor Create empty Asm db file
 */
data class AsmDbFile(
    var name: String = "",
    var is_used: Boolean = false,
    var index: Int = 0,
    var file_type: String = "",
    var blocks: Int = 0,
    var block_size: Int = 0,
    var file_handle: Int = 0,
    var physical_block_size: Int = 0,
    var off: Int = 0,
    var file_size: Int = 0,
    var con: Connection? = null)

/**
 * Asm disk
 *
 * @property group_no 组号
 * @property disk_no 磁盘号
 * @property path 路径
 * @property dev
 * @property fd 句柄
 * @constructor Create empty Asm disk
 */
data class AsmDisk(val group_no: Int,
                   val disk_no: Int,
                   val path: String,
                   var dev: String = "",
                   var fd: Int = -1)

data class AsmDevMap(val disk: String = "", val dev: String = "")

typealias AU = MutableList<Au>

/** au of disk */
data class Au(val disk: AsmDisk, val au_num: Int)
